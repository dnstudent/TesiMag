library(dplyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

test_data <- function(data_ds) {
    data_ds |>
        group_by(series_id, date) |>
        count()
}

id_consistency <- function(data, id_col, naming_fn, ...) {
    edit <- naming_fn(data, ...)
    if (!all(pull(data, all_of(id_col)) == pull(edit, all_of(id_col)))) {
        warn(paste("Id consistency failed for column", id_col))
    } else {
        print(paste("Everything consistent in", id_col))
    }
}
