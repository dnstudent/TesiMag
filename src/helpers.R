# Description: Helper functions for the project

#' Count NA values in a vector
#'
#' @param x The vector to count NA values from
#' @return The number of NA values in the vector
count_nas.numeric <- function(x) {
    sum(is.na(x))
}

#' Count NA values in a vector
#'
#' @param x A boolean vector representing whether a value is NA or not
#' @return The number of NA values in the vector
count_nas.logical <- function(x) {
    sum(x)
}

count_nas <- function(x) {
    UseMethod("count_nas", x)
}

#' Count the maximum number of consecutive NA values in a vector
#'
#' @param x A logical vector representing whether a value is NA or not
#' @return The maximum number of consecutive NA values in the vector
maximum_nas.logical <- function(x) {
    with(rle(x), ifelse(any(values), max(lengths[values]), 0))
}

maximum_nas <- function(x) {
    UseMethod("maximum_nas", x)
}
