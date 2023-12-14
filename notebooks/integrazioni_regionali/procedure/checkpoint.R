library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/database/write.R")
source("src/database/open.R")
source("src/load/tools.R")

checkpoint_database <- function(database, dataset_id, tag) {
    database$data <- arrange(database$data, station_id, variable, date)
    write_data(database$data, dataset_id, tag, provisional = TRUE)
    write_data(database$data, dataset_id, "last", provisional = TRUE)
    write_metadata(database$meta, dataset_id, tag, provisional = TRUE)
    write_metadata(database$meta, dataset_id, "last", provisional = TRUE)
}

open_checkpoint <- function(dataset_id, tag) {
    list(
        "meta" = read_metadata(dataset_id, tag, provisional = TRUE),
        "data" = open_data(dataset_id, tag, provisional = TRUE)
    )
}

open_last_checkpoint <- function(dataset_id) {
    list(
        "meta" = read_metadata(dataset_id, "last", provisional = TRUE),
        "data" = open_data(dataset_id, "last", provisional = TRUE)
    )
}
