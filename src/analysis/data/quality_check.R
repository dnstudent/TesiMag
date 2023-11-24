# Quality check functions for data.
# Assumption is that data is a tsibble, with temperatures in Celsius in the 'value' column.

#' Test for gross errors in data.
gross_errors_check <- function(data) {
    data |> mutate(qc_gross = abs(value) > 50)
}

more_consecutive_than <- function(x, n) {
    rles <- rle(x)
    rles$values <- rles$lengths >= n
    inverse.rle(rles)
}

#' Tests for repeated or missing values in data: if 'value' is constant or missing for seven consecutive days, it is flagged.
repeated_values_check <- function(data) {
    data |>
        drop_na() |>
        arrange(variable, identifier, date) |>
        group_by_key() |>
        mutate(qc_repeated = more_consecutive_than(value, 7))
}
