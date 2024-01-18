library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)
library(duckdb, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/load/tools.R")

read_storage <- function(dataset, what, step) {
    read_parquet(archive_path(dataset, what, step))
}

open_storage <- function(dataset, what, step) {
    open_dataset(archive_path(dataset, what, step))
}
