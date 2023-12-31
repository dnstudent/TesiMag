library(dplyr, warn.conflicts = FALSE)

# Tests for single series errors in data.


# Function: gross_errors_check.numeric
# Description: This function checks for gross errors in numeric values.
# Parameters:
#   - value: The numeric value to be checked.
#   - thresh: The threshold value for identifying gross errors (default = 50).
# Returns:
#   TRUE if the value is considered a gross error, FALSE otherwise.
gross_errors_check.numeric <- function(value, thresh = 50) {
    abs(value) >= thresh
}

gross_errors_check.arrow_dplyr_query <- function(query, value_col, thresh = 50) {
    mutate(query, qc_gross = abs({{ value_col }}) >= thresh)
}

gross_errors_check.tbl_lazy <- function(query, value_col, thresh = 50) {
    mutate(query, qc_gross = abs({{ value_col }}) >= thresh)
}

gross_errors_check.ArrowObject <- function(query, value_col, thresh = 50) {
    mutate(query, qc_gross = abs({{ value_col }}) >= thresh)
}


#' Check for Gross Errors in a Data Frame
#'
#' This function checks for gross errors in a data frame by comparing the values
#' against a threshold. Any values that exceed the threshold are considered as
#' gross errors.
#'
#' @param data The data frame to be checked.
#' @param thresh The threshold value for identifying gross errors. Default is 50.
#'
#' @return A logical vector indicating whether each value in the data frame is a
#'         gross error or not.
#'
#' @examples
#' data <- data.frame(x = c(10, 20, 30, 100, 40), y = c(50, 60, 70, 80, 90))
#' gross_errors_check.data.frame(data, thresh = 50)
#'
#' @export
gross_errors_check.data.frame <- function(data, thresh = 50) {
    data |> mutate(qc_gross = gross_errors_check.numeric(value, thresh))
}

gross_errors_check <- function(x, ...) UseMethod("gross_errors_check", x)


#' Check for repeated values in numeric data
#'
#' This function checks for repeated values in a numeric vector.
#'
#' @param x A numeric vector.
#' @param threshold The maximum number of allowed repeated values.
#'
#' @return TRUE if the number of repeated values is less than or equal to the threshold, FALSE otherwise.
#'
#' @examples
#' repeated_values_check.numeric(c(1, 2, 3, 3, 4), 2)
#' # Output: FALSE
#'
#' repeated_values_check.numeric(c(1, 2, 3, 3, 4), 3)
#' # Output: TRUE
#'
repeated_values_check.numeric <- function(x, threshold) {
    rles <- rle(x)
    rles$values <- rles$lengths >= threshold
    inverse.rle(rles)
}

#' Tests for repeated or missing values in data: if 'value' is constant or missing for seven consecutive days, it is flagged. WATCH OUT: GAPS SHOULD NOT BE FILLED
repeated_values_check.data.frame <- function(data) {
    if (!is.grouped_df(data)) {
        warning("The data provided is not grouped. Is this intentional?")
    }
    data |>
        drop_na(value) |>
        arrange(date, .by_group = TRUE) |>
        mutate(qc_repeated = repeated_values_check.numeric(value, 7))
}

repeated_values_check.tbl_sql <- function(conn, table_name) {
    query <- readr::read_file("src/database/query/n_consecutive.sql") |> glue::glue_sql(.con = conn)
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
        drop_na(value) |>
        summarise(qc_repeated_fraction = repeated_fraction_check.numeric(value))
}

repeated_fraction_check.tbl_lazy <- function(data) {
    data |>
        filter(!is.na(value)) |>
        summarise(qc_repeated_fraction = repeated_fraction_check.numeric(value))
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
        summarise(qc_integer_fraction = integers_fraction_check.numeric(value))
}

integers_fraction_check <- function(x, ...) UseMethod("integers_fraction_check", x)

integer_streak_check.numeric <- function(x, threshold) {
    rles <- rle(abs(x - trunc(x)) <= 1e-4)
    rles$values <- (rles$values & (rles$lengths >= threshold))
    inverse.rle(rles)
}

integer_streak_check.data.frame <- function(data, threshold = 8L) {
    if (!is.grouped_df(data)) {
        warning("The data provided is not grouped. Is this intentional?")
    }
    data |>
        mutate(qc_int_streak = integer_streak_check.numeric(value, threshold))
}

integer_streak_check.tbl_lazy <- function(data, threshold = 8L) {
    data |>
        mutate(qc_int_streak = integer_streak_check.numeric(value, threshold))
}

integer_streak_check <- function(x, ...) UseMethod("integer_streak_check", x)
