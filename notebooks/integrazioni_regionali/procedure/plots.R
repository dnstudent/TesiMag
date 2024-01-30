library(ggplot2, warn.conflicts = FALSE)
library(tsibble, warn.conflicts = FALSE)
options(repr.plot.width = 9, repr.plot.res = 300)

source("src/analysis/data/clim_availability.R")

plot_state_avail <- function(metadata, data, start_date = NULL, end_date = NULL, ...) {
    ymonthly_availabilities <- data |>
        left_join(metadata |> select(dataset, sensor_key), join_by(dataset, sensor_key)) |>
        collect() |>
        as_tsibble(key = c(dataset, sensor_key, variable), index = date) |>
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

plot_state_avail.tbl <- function(data, .minimum_valid_days = 20L, .maximum_consecutive_missing_days = 4L) {
    ymonthly <- monthly_availabilities(data, minimum_valid_days = .minimum_valid_days, maximum_consecutive_missing_days = .maximum_consecutive_missing_days) |>
        compute()

    p <- ymonthly |>
        group_by(dataset, variable, year, month, .add = TRUE) |>
        summarise(available_series = sum(if_else(qc_month_available, 1L, 0L), na.rm = TRUE), .groups = "drop") |>
        arrange(year, month) |>
        collect() |>
        mutate(yearmonth = make_yearmonth(year, month)) |>
        as_tsibble(key = c(variable, dataset), index = yearmonth) |>
        ggplot() +
        geom_line(aes(yearmonth, available_series, color = dataset))
    list("plot" = p, "data" = ymonthly)
}
