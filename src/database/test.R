library(dplyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

assert_data_uniqueness <- function(checkpoint) {
    if (checkpoint$data |>
        group_by(station_id, variable, date) |>
        tally() |>
        filter(n > 1L) |>
        compute() |>
        nrow() > 0L) {
        stop("Data entries are not unique")
    }
    checkpoint
}

assert_metadata_uniqueness <- function(checkpoint) {
    if (checkpoint$meta |>
        group_by(original_dataset, original_id) |>
        tally() |>
        filter(n > 1L) |>
        compute() |>
        nrow() > 0L) {
        stop("Metadata entries are not unique")
    }
    checkpoint
}

id_consistency <- function(data, id_col, naming_fn, ...) {
    edit <- naming_fn(data, ...)
    if (!all(pull(data, all_of(id_col)) == pull(edit, all_of(id_col)))) {
        warn(paste("Id consistency failed for column", id_col))
    } else {
        print(paste("Everything consistent in", id_col))
    }
}
