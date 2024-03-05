library(ggplot2, warn.conflicts = FALSE)
library(pals, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
source("src/database/query/data.R")
source("src/merging/pairing.R")
source("src/merging/combining.R")

plot_stations <- function(ids, data, same_period = TRUE, matches = TRUE) {
    cols <- colnames(ids)
    data <- data |>
        semi_join(ids, by = cols, copy = TRUE) |>
        collect() |>
        arrange(sensor_key, date)

    if (same_period) {
        first_date <- data |>
            group_by(dataset, sensor_key) |>
            summarize(first_common_date = min(date, na.rm = TRUE), .groups = "drop") |>
            pull(first_common_date) |>
            max()

        last_date <- data |>
            group_by(dataset, sensor_key) |>
            summarize(last_common_date = max(date, na.rm = TRUE), .groups = "drop") |>
            pull(last_common_date) |>
            min()
    } else {
        first_date <- data |>
            filter(!is.na(value)) |>
            pull(date) |>
            min()
        last_date <- data |>
            filter(!is.na(value)) |>
            pull(date) |>
            max()
    }
    ggplot(data = data |> mutate(sensor_key = as.factor(sensor_key)) |> filter(first_date <= date & date <= last_date)) +
        geom_line(aes(x = date, y = value, color = sensor_key, linetype = sensor_key)) +
        facet_grid(variable ~ .)
}

plot_diffs <- function(matches, data, ...) {
    dataconn <- data$src$con
    matches <- copy_to(dataconn, matches, overwrite = TRUE, name = "matches_plot_tmp")
    if (!("offset_days" %in% colnames(matches))) {
        matches <- matches |>
            mutate(offset_days = 0L)
    }

    data <- pair_common_series(data, matches) |>
        left_join(matches |> select(starts_with("dataset"), starts_with("sensor_key"), starts_with("key_"), variable, starts_with("name"), starts_with("sensor_id")), by = c("key_x", "key_y", "variable")) |>
        mutate(delT = value_y - value_x, valid = !is.na(delT)) |>
        collect() |>
        mutate(
            variable = factor(variable, levels = c(-1L, 1L), labels = c("T_MIN", "T_MAX")),
            match_id = as.factor(paste0(dataset_x, " ", name_x, "\n", dataset_y, " ", name_y))
        )

    dbExecute(dataconn, "DROP TABLE matches_plot_tmp")

    ggplot(data = data |> arrange(match_id, variable, date)) +
        geom_line(aes(x = date, y = delT, color = variable, linetype = variable, ...), na.rm = TRUE) +
        scale_color_manual(values = coolwarm(2)) +
        facet_grid(match_id ~ ., scales = "free")
}

plot_correction <- function(corrections, metadata, data, ...) {
    if (!("offset_days" %in% colnames(corrections))) {
        corrections <- corrections |>
            mutate(offset_days = 0L)
    }
    corrections <- corrections |>
        select(key_x, key_y, variable, k0, k1, k2, k3, offset_days)

    diffs <- pair_common_series(data, corrections, by = c("key_x", "key_y", "variable")) |>
        collect() |>
        left_join(corrections |> select(-offset_days), by = c("key_x", "key_y", "variable")) |>
        left_join(metadata |> select(key, dataset, name), by = c("key_x" = "key")) |>
        left_join(metadata |> select(key, dataset, name), by = c("key_y" = "key"), suffix = c("_x", "_y")) |>
        mutate(
            variable = factor(variable, levels = c(-1L, 1L), labels = c("T_MIN", "T_MAX"), ordered = TRUE),
            delT = value_x - value_y,
            t = annual_index(date),
            match_id = as.factor(paste0(dataset_x, " ", name_x, "\n", dataset_y, " ", name_y)),
            correction = k0 + sin(t / 2) * k1 + sin(t) * k2 + sin(2 * t) * k3
        )

    monthly <- diffs |>
        group_by(match_id, variable, key_x, key_y, year = year(date), month = month(date)) |>
        summarise(
            delT = mean(delT, na.rm = TRUE),
            valid = n() >= 15L,
            .groups = "drop"
        ) |>
        mutate(
            date = make_date(year, month, 15L),
        )

    ggplot() +
        geom_point(data = monthly, aes(x = date, y = delT, color = variable, shape = valid, ...), na.rm = TRUE) +
        geom_line(data = diffs, aes(x = date, y = correction, color = variable, ...), na.rm = TRUE) +
        scale_color_manual(values = coolwarm(2L)) +
        facet_grid(match_id ~ ., scales = "free")
}

plot_random_matches <- function(matches, dataconn, ..., n = 5L) {
    combos <- matches |>
        filter(...) |>
        slice_sample(n = n) |>
        select(id_x, id_y, variable)

    matches |>
        semi_join(combos, by = c("id_x", "id_y")) |>
        plot_diffs(dataconn)
}

plot_alldiffs <- function(matches, dataconn, ..., out = "alldiffs.pdf") {
    relevant <- matches |> filter(...)
    p <- relevant |>
        plot_diffs(dataconn)

    ggsave(out, p, height = nrow(relevant) * 2, width = 9, limitsize = FALSE)
}
