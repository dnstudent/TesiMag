library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

read_station_metadata <- function(dataset_id) {
    read_parquet(file.path("db", "metadata", "stations", paste0(dataset_id, ".parquet")), as_data_frame = FALSE)
}

read_station_extra_metadata <- function(dataset_id) {
    read_parquet(file.path("db", "metadata", "extra", paste0(dataset_id, ".parquet")), as_data_frame = FALSE)
}

read_series_metadata <- function(dataset_id) {
    read_parquet(file.path("db", "metadata", "series", paste0(dataset_id, ".parquet")), as_data_frame = FALSE)
}

open_data <- function(dataset_id) {
    open_dataset(file.path("db", "data", paste0(dataset_id, ".parquet")))
}
