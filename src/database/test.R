library(dplyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

assert_data_uniqueness <- function(checkpoint, keys) {
    if (checkpoint$data |>
        group_by(across(all_of(keys)), variable, date) |>
        tally() |>
        filter(n > 1L) |>
        compute() |>
        nrow() > 0L) {
        stop("Data entries are not unique")
    }
    checkpoint
}

assert_metadata_uniqueness <- function(checkpoint, keys) {
    if (checkpoint$meta |>
        group_by(across(all_of(keys))) |>
        tally() |>
        filter(n > 1L) |>
        compute() |>
        nrow() > 0L) {
        stop("Metadata entries are not unique")
    }
    checkpoint
}

warn_more_entries <- function(checkpoint, keys) {
    if (checkpoint$data |> select(all_of(keys)) |> anti_join(checkpoint$meta |> select(all_of(keys)), by = keys) |> compute() |> nrow() > 0L) {
        warn("More entries in data than in metadata")
    }
    if (checkpoint$meta |> select(all_of(keys)) |> anti_join(checkpoint$data |> select(all_of(keys)), by = keys) |> compute() |> nrow() > 0L) {
        warn("More entries in metadata than in data")
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
