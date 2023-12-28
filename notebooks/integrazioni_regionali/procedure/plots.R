library(ggplot2, warn.conflicts = FALSE)
library(tsibble, warn.conflicts = FALSE)
options(repr.plot.width = 9, repr.plot.res = 300)

source("src/analysis/data/clim_availability.R")

plot_state_avail <- function(metadata, data, start_date = NULL, end_date = NULL, ...) {
    ymonthly_availabilities <- data |>
        left_join(metadata |> select(dataset, id), join_by(dataset, station_id == id)) |>
        collect() |>
        as_tsibble(key = c(dataset, station_id, variable), index = date) |>
        is_month_usable(value, .start = start_date, .end = end_date, groups = "drop", ...)
    p <- ymonthly_availabilities |>
        arrange(dataset, variable, year_month) |>
        group_by(dataset, variable) |>
        index_by(year_month) |>
        summarise(n_available_series = sum(available)) |>
        ggplot(aes(year_month, n_available_series, color = dataset)) +
        geom_line() +
        facet_grid(variable ~ .)
    list("plot" = p, "data" = ymonthly_availabilities)
}

climats_availabilities.arrow <- function(data, series_meta, station_meta, state, start_date, end_date) {
    state_boundaries <- load.italian_boundaries("state") |> filter(shapeName == state)
    climats_comp <- is_climatology_computable(
        as_tsibble(
            left_join(data, series_meta |> select(series_id, variable), by = "series_id") |> collect(),
            key = c(series_id, variable), index = date
        ),
        value,
        .start = first_date, .end = last_date
    ) |>
        left_join(series_meta |> select(series_id, station_id) |> collect(), by = "series_id") |>
        left_join(station_meta |> select(station_id, lon, lat) |> collect(), by = "station_id") |>
        st_md_to_sf()
}
