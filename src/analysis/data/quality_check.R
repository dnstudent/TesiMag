# Quality check functions for data.
# Assumption is that data is a tsibble, with temperatures in Celsius in the 'value' column.

gross_errors_check.arrow_dplyr_query <- function(query, value_col, thresh = 50) {
    mutate(query, qc_gross = abs({{ value_col }}) >= thresh)
}

gross_errors_check.numeric <- function(value, thresh = 50) {
    abs(value) >= thresh
}

#' Test for gross errors in data.
gross_errors_check.data.frame <- function(data, thresh = 50) {
    data |> mutate(qc_gross = gross_errors_check.numeric(value, thresh))
}

gross_errors_check <- function(x, ...) UseMethod("gross_errors_check", x)

repeated_values_check.numeric <- function(x, n) {
    rles <- rle(x)
    rles$values <- rles$lengths >= n
    inverse.rle(rles)
}

#' Tests for repeated or missing values in data: if 'value' is constant or missing for seven consecutive days, it is flagged. WATCH OUT: GAPS SHOULD NOT BE FILLED
repeated_values_check.tbl_ts <- function(data) {
    data |>
        drop_na() |>
        group_by_key() |>
        arrange(date, .by_group = TRUE) |>
        mutate(qc_repeated = repeated_values_check.numeric(value, 7))
}

repeated_values_check <- function(x, ...) UseMethod("repeated_values_check", x)
