# Quality check functions for data.
# Assumption is that data is a tsibble, with temperatures in Celsius in the 'value' column.

gross_errors_check.arrow_dplyr_query <- function(query, value_col, thresh = 50) {
    mutate(query, qc_gross = abs({{ value_col }}) >= thresh)
}

gross_errors_check.ArrowObject <- function(query, value_col, thresh = 50) {
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
repeated_values_check.data.frame <- function(data) {
    if (!is.grouped_df(data)) {
        warning("The data provided is not grouped. Is this intentional?")
    }
    data |>
        drop_na(value) |>
        mutate(qc_repeated = repeated_values_check.numeric(value, 7))
}

repeated_values_check <- function(x, ...) UseMethod("repeated_values_check", x)

repeated_fraction_check.numeric <- function(x) {
    rles <- rle(x)
    sum(rles$lengths[rles$lengths > 1]) / length(x)
}

repeated_fraction_check.data.frame <- function(data) {
    if (!is.grouped_df(data)) {
        warning("The data provided is not grouped. Is this intentional?")
    }
    data |>
        summarise(qc_repeated_fraction = repeated_fraction_check.numeric(.data[["value"]] |> drop_na()))
}

repeated_fraction_check <- function(x, ...) UseMethod("repeated_fraction_check", x)

integers_fraction_check.numeric <- function(x) {
    mean(abs(x - trunc(x)) <= 1e-4, na.rm = TRUE)
}

integers_fraction_check.data.frame <- function(data) {
    if (!is.grouped_df(data)) {
        warning("The data provided is not grouped. Is this intentional?")
    }
    data |>
        summarise(qc_integer_fraction = integers_fraction_check.numeric(.data[["value"]]))
}

integers_fraction_check <- function(x, ...) UseMethod("integers_fraction_check", x)

integer_streak_check.numeric <- function(x, threshold) {
    rles <- rle(abs(x - trunc(x)) <= 1e-4)
    rles$values <- (rles$values & (rles$lengths >= threshold))
    inverse.rle(rles)
}

integer_streak_check.data.frame <- function(data, threshold = 8) {
    if (!is.grouped_df(data)) {
        warning("The data provided is not grouped. Is this intentional?")
    }
    data |>
        mutate(qc_int_streak = integer_streak_check.numeric(value, threshold))
}

integer_streak_check <- function(x, ...) UseMethod("integer_streak_check", x)
