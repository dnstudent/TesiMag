library(ggplot2, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
source("src/database/query/data.R")
source("src/database/query/pairing.R")

plot_stations <- function(ids, dataconn) {
    data <- valid_data(dataconn) |>
        semi_join(ids, by = colnames(ids), copy = TRUE) |>
        arrange(station_id, date) |>
        collect()

    first_common_date <- data |>
        group_by(station_id) |>
        summarize(first_common_date = min(date, na.rm = TRUE)) |>
        pull(first_common_date) |>
        max()

    last_common_date <- data |>
        group_by(station_id) |>
        summarize(last_common_date = max(date, na.rm = TRUE)) |>
        pull(last_common_date) |>
        min()

    ggplot(data = data |> mutate(station_id = as.factor(station_id)) |> filter(first_common_date <= date & date <= last_common_date)) +
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
        mutate(delT = value_y - value_x) |>
        collect()

    dbExecute(dataconn, "DROP TABLE matches_plot_tmp")

    ggplot(data = data |> filter(!is.na(delT))) +
        geom_line(aes(x = date, y = delT, ...)) +
        facet_grid(variable ~ ., scales = "free_y")
}
