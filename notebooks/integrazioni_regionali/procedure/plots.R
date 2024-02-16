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

plot_state_avail.tbl <- function(data, stack, .minimum_valid_days = 20L, .maximum_consecutive_missing_days = 4L) {
    ymonthly <- monthly_availabilities(data, minimum_valid_days = .minimum_valid_days, maximum_consecutive_missing_days = .maximum_consecutive_missing_days) |>
        compute()

    p <- ymonthly |>
        group_by(dataset, variable, year, month, .add = TRUE) |>
        summarise(available_series = sum(if_else(qc_month_available, 1L, 0L), na.rm = TRUE), .groups = "drop") |>
        arrange(year, month) |>
        collect() |>
        mutate(yearmonth = make_yearmonth(year, month)) |>
        as_tsibble(key = c(variable, dataset), index = yearmonth) |>
        ggplot()

    if (stack) {
        p <- p + geom_area(aes(yearmonth, available_series, fill = dataset), position = "stack")
    } else {
        p <- p + geom_line(aes(yearmonth, available_series, color = dataset))
    }
    list("plot" = p, "data" = ymonthly)
}

plot_available_by_elevation <- function(clim_availabilities, metadata, ...) {
    clim_availabilities |>
        filter(qc_clim_available) |>
        mutate(variable = case_match(variable, -1L ~ "T_MIN", 1L ~ "T_MAX")) |>
        left_join(metadata |> select(dataset, sensor_key, elevation) |> collect(), join_by(dataset, sensor_key)) |>
        ggplot() +
        geom_histogram(aes(elevation, fill = variable, ...), binwidth = 100, boundary = 0, position = "dodge")
}
