library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

base_path <- function(section, provisional) {
    path <- file.path("db", section)
    if (provisional) {
        path <- file.path(path, "intermediate")
    }
    path
}

read_station_metadata <- function(dataset_id, provisional) {
    read_parquet(file.path(base_path("metadata", provisional), "stations", paste0(dataset_id, ".parquet")), as_data_frame = FALSE)
}

read_station_extra_metadata <- function(dataset_id, provisional) {
    read_parquet(file.path(base_path("metadata", provisional), "extra", paste0(dataset_id, ".parquet")), as_data_frame = FALSE)
}

read_series_metadata <- function(dataset_id, provisional) {
    read_parquet(file.path(base_path("metadata", provisional), "series", paste0(dataset_id, ".parquet")), as_data_frame = FALSE)
}

open_data <- function(dataset_id, provisional) {
    open_dataset(file.path(base_path("data", provisional), paste0(dataset_id, ".parquet")))
}
