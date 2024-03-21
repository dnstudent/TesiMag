library(arrow, warn.conflicts = FALSE)

source("src/database/tools.R")

read_storage <- function(dataset, what, step) {
    read_parquet(archive_path(dataset, what, step))
}

open_storage <- function(dataset, what, step) {
    open_dataset(archive_path(dataset, what, step))
}
