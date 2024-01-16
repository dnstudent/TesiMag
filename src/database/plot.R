library(ggplot2, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
source("src/database/query/data.R")
source("src/merging/pairing.R")

plot_stations <- function(ids, dataconn, same_period = TRUE) {
    data <- valid_data(dataconn) |>
        semi_join(ids, by = colnames(ids), copy = TRUE) |>
        arrange(station_id, date) |>
        collect()

    if (same_period) {
        first_date <- data |>
            group_by(station_id) |>
            summarize(first_common_date = min(date, na.rm = TRUE)) |>
            pull(first_common_date) |>
            max()

        last_date <- data |>
            group_by(station_id) |>
            summarize(last_common_date = max(date, na.rm = TRUE)) |>
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
    ggplot(data = data |> mutate(station_id = as.factor(station_id)) |> filter(first_date <= date & date <= last_date)) +
        geom_line(aes(x = date, y = value, color = station_id)) +
        facet_grid(variable ~ .)
}

plot_diffs <- function(matches, dataconn, ...) {
    matches <- copy_to(dataconn, matches, overwrite = TRUE, name = "matches_plot_tmp")
    if (!("offset_days" %in% colnames(matches))) {
        matches <- matches |>
            mutate(offset_days = 0L)
    }

    data <- valid_data(dataconn)
    data <- pair_full_series(data, matches) |>
        left_join(matches |> select(starts_with("id"), variable, starts_with("dataset"), starts_with("name")), by = c("id_x", "id_y", "variable")) |>
        mutate(delT = value_y - value_x) |>
        collect() |>
        filter(!is.na(delT)) |>
        mutate(
            variable = factor(variable, levels = c(-1L, 1L), labels = c("T_MIN", "T_MAX")),
            match_id = as.factor(paste0(dataset_x, " ", name_x, " ", id_x, "\n", dataset_y, " ", name_y, " ", id_y))
        )

    dbExecute(dataconn, "DROP TABLE matches_plot_tmp")

    ggplot(data = data) +
        geom_line(aes(x = date, y = delT, color = variable, linetype = variable, ...)) +
        facet_grid(match_id ~ ., scales = "free")
}

plot_correction <- function(corrections, dataconn, ...) {
    p <- plot_diffs(corrections, dataconn, ...)
    c_data <- p$data |>
        select(variable, starts_with("id"), date) |>
        left_join(corrections |> mutate(variable = if_else(variable == -1L, "T_MIN", "T_MAX")), by = c("variable", "id_x", "id_y")) |>
        mutate(t = annual_index(date), correction = k0 + sin(t / 2) * k1 + sin(t) * k2 + sin(3 * t / 2) * k3)
    p + geom_line(data = c_data, aes(x = date, y = correction, color = variable, ...))
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
