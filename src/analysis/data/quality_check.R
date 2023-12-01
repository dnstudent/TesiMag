# Quality check functions for data.
# Assumption is that data is a tsibble, with temperatures in Celsius in the 'value' column.

gross_errors_check.arrow_dplyr_query <- function(query, value_col) {
    mutate(query, qc_gross = abs({{value_col}}) > 50)
}

gross_errors_check.numeric <- function(value) {
    abs(value) > 50
}

#' Test for gross errors in data.
gross_errors_check.data.frame <- function(data) {
    data |> mutate(qc_gross = gross_errors_check.numeric(value))
}

gross_errors_check <- function(x, ...) UseMethod("gross_errors_check", x)

repeated_values_check.numeric <- function(x, n) {
    rles <- rle(x)
    rles$values <- rles$lengths >= n
    inverse.rle(rles)
}

#' Tests for repeated or missing values in data: if 'value' is constant or missing for seven consecutive days, it is flagged.
repeated_values_check.tbl_ts <- function(data) {
    data |>
        drop_na() |>
        arrange(variable, identifier, date) |>
        group_by_key() |>
        mutate(qc_repeated = repeated_values_check.numeric(value, 7))
}

repeated_values_check <- function(x, ...) UseMethod("repeated_values_check", x)
