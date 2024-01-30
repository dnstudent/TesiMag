library(lubridate, warn.conflicts = FALSE)
library(tsibble, warn.conflicts = FALSE)

source("src/helpers.R")

filter_data <- function(data, .start = NULL, .end = NULL) {
    if (!is.null(.start)) {
        data <- data |> filter(.start <= date)
    }
    if (!is.null(.end)) {
        data <- data |> filter(date <= .end)
    }
    data
}

is_month_usable.logical <- function(is_na, max_na_days = 10, max_consecutive_nas = 4) {
    (count_nas(is_na) <= max_na_days) && (with(rle(is_na), all(lengths[values] <= max_consecutive_nas)))
}

#' Assess the usability of a complete monthly series of data for climate normal computation according to WMO standards
#'
#' @param series The time series of recorded values for a given month (e.g. 1 Jan 2005: 2.3 °C, 2 Jan 2005: 3.3 °C, 3 Jan 2005: 2.1 °C, ..., 31 Jan 2005: 1.2 °C)
#' @return A boolean value representing whether the series is available or not
is_month_usable.numeric <- function(series, max_na_days = 10, max_consecutive_nas = 4) {
    is_month_usable.logical(is.na(series), max_na_days, max_consecutive_nas)
}

is_month_usable.tbl_ts <- function(data, variable, .start = NULL, .end = NULL, max_na_days = 10, max_consecutive_nas = 4, groups = NULL) {
    filter_data(data, .start, .end) |>
        fill_gaps(.full = TRUE, .start = .start, .end = .end) |>
        group_by_key() |>
        index_by(year_month = ~ yearmonth(.)) |>
        summarise(available = is_month_usable.logical(is.na({{ variable }}), max_na_days, max_consecutive_nas), .groups = groups)
}

is_month_usable <- function(x, ...) UseMethod("is_month_usable", x)

monthly_availabilities.grouped <- function(table, minimum_valid_days = 20L, maximum_consecutive_missing_days = 4L) {
    table |>
        mutate(datediff = (as.integer(date - lag(date, order_by = date))) |> coalesce(1L)) |>
        summarise(qc_month_available = (n() >= minimum_valid_days & max(datediff, na.rm = TRUE) <= maximum_consecutive_missing_days), .groups = "drop")
}

monthly_availabilities <- function(table, minimum_valid_days = 20L, maximum_consecutive_missing_days = 4L) {
    table |>
        filter(!is.na(value)) |>
        group_by(dataset, sensor_key, variable, month = as.integer(month(date)), year = as.integer(year(date)), .add = TRUE) |>
        monthly_availabilities.grouped(minimum_valid_days, maximum_consecutive_missing_days)
}

#' Assess the usability of a complete series of monthly availabilities (as returned by \code{is.month.usable}) for climate normal computation according to WMO standards
#'
#' @param monthly_availability The vector of availabilities for a given month (e.g.: Jan 2005: TRUE, Jan 2006: TRUE, Jan 2007: FALSE, ...)
#' @return A boolean value representing whether the data at hand is usable to compute a climate normal or not
is_climatology_computable.logical <- function(monthly_availability, n_years_minimum = 10) {
    sum(monthly_availability) >= n_years_minimum
}

#' Assess the usability of a complete series of daily data for climate normal computation according to WMO standards.
#'
#' @param data The time series of recorded values for a given month. It must be a \code{tsibble} object without gaps (complete).
#' @param variable The name of the variable to assess the availability of.
#' @return A boolean \code{tsibble} representing whether the data at hand is usable to compute a climate normal or not
is_climatology_computable.tbl_ts <- function(data, variable, .start = NULL, .end = NULL, max_na_days = 10, max_consecutive_nas = 4, n_years_minimum = 10, monthly_usabilities = FALSE) {
    if (!monthly_usabilities) {
        data <- data |>
            is_month_usable.tbl_ts({{ variable }}, .start, .end, max_na_days, max_consecutive_nas, groups = "keep")
    }
    data |>
        group_by_key() |>
        index_by(month = ~ month(., label = TRUE)) |>
        summarise(clim_available = is_climatology_computable.logical(available, n_years_minimum))
}

is_climatology_computable.series <- function(s1, s2, dates, max_na_days = 10, max_consecutive_nas = 4, n_years_minimum = 10) {
    ina <- is.na(s1) & is.na(s2)
    bind_cols(series = na_if(ina, TRUE), date = dates) |>
        as_tsibble(index = date) |>
        is_climatology_computable.tbl_ts(series, max_na_days = max_na_days, max_consecutive_nas = max_consecutive_nas, n_years_minimum = n_years_minimum)
}

is_climatology_computable <- function(x, ...) UseMethod("is_climatology_computable", x)

clim_availability.grouped <- function(monthly_availabilities, n_years_threshold = 10L) {
    monthly_availabilities |>
        summarise(qc_clim_available = sum(as.integer(qc_month_available), na.rm = TRUE) >= n_years_threshold, .groups = "drop_last") |>
        summarise(qc_clim_available = all(qc_clim_available, na.rm = TRUE), .groups = "drop")
}

clim_availability <- function(monthly_availabilities, n_years_threshold = 10L) {
    monthly_availabilities |>
        group_by(dataset, sensor_key, variable, month) |>
        clim_availability.grouped(n_years_threshold)
}
