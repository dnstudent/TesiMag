library(dplyr, warn.conflicts = FALSE)

swap_cols <- function(table, col1, col2) {
    if (col1 == col2) {
        return(table)
    }
    orig_order <- table |> colnames()
    table |>
        rename(first = all_of(col1), "{col1}" := all_of(col2)) |>
        rename("{col2}" := first) |>
        relocate(all_of(orig_order))
}

#' Checks whether a table is symmetric with respect to two given columns.
#'
#' @param table A table.
#' @param col1 A column name.
#' @param col2 A column name.
#'
#' @return `TRUE` if the table is symmetric with respect to the two given columns, `FALSE` otherwise.
is_symmetric <- function(table, col1, col2) {
    inverted <- swap_cols(table, col1, col2)
    (anti_join(table, inverted, by = c(col1, col2)) |>
        compute() |>
        nrow()) == 0L
}

#' Check if a table contains symmetric duplicates based on two columns.
#'
#' @param table The table to check for symmetric duplicates.
#' @param col1 The name of the first column.
#' @param col2 The name of the second column.
#' @return A logical value indicating whether the table contains symmetric duplicates.
contains_symmetric_duplicates <- function(table, col1, col2) {
    inverted <- swap_cols(table, col1, col2)
    (semi_join(table, inverted, by = c(col1, col2, "variable")) |>
        compute() |>
        nrow()) > 0L
}
