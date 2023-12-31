library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/load/tools.R")

read_station_metadata <- function(dataset_id, provisional) {
    read_parquet(file.path(base_path("metadata", provisional), "stations", paste0(dataset_id, ".parquet")), as_data_frame = FALSE)
}

read_station_extra_metadata <- function(dataset_id, provisional) {
    read_parquet(file.path(base_path("metadata", provisional), "extra", paste0(dataset_id, ".parquet")), as_data_frame = FALSE)
}

read_series_metadata <- function(dataset_id, provisional) {
    read_parquet(file.path(base_path("metadata", provisional), "series", paste0(dataset_id, ".parquet")), as_data_frame = FALSE)
}

open_data <- function(dataset_id, tag, provisional) {
    open_dataset(file.path(base_path("data", provisional), dataset_id, paste0(tag, ".parquet")))
}

open_metadata <- function(dataset_id, tag, provisional) {
    open_dataset(file.path(base_path("metadata", provisional), dataset_id, paste0(tag, ".parquet")))
}

read_extra_metadata <- function(dataset_id, provisional) {
    read_parquet(file.path(base_path("metadata", provisional), dataset_id, paste0("extra.parquet")), as_data_frame = FALSE)
}
