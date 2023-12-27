library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/database/write.R")
source("src/database/open.R")
source("src/database/test.R")
source("src/load/tools.R")

checkpoint_database <- function(database, dataset_id, tag) {
    database$data <- arrange(database$data, dataset, station_id, variable, date)
    database |>
        assert_data_uniqueness() |>
        assert_metadata_uniqueness()
    write_data(database$data, dataset_id, tag, provisional = TRUE)
    write_data(database$data, dataset_id, "last", provisional = TRUE)
    write_metadata(database$meta, dataset_id, tag, provisional = TRUE)
    write_metadata(database$meta, dataset_id, "last", provisional = TRUE)
}

open_checkpoint <- function(dataset_id, tag) {
    list(
        "meta" = read_metadata(dataset_id, tag, provisional = TRUE),
        "data" = open_data(dataset_id, tag, provisional = TRUE)
    ) |> as_database()
}

open_last_checkpoint <- function(dataset_id) {
    open_checkpoint(dataset_id, "last")
}

#' Writes a match table to disk.
save_match_list <- function(match_list, dataset_id, tag) {
    table_path <- file.path(base_path("matches", FALSE), dataset_id, paste0(tag, ".parquet"))
    if (!dir.exists(dirname(table_path))) {
        dir.create(dirname(table_path), recursive = TRUE)
    }
    write_parquet(match_list, table_path)
}

load_match_list <- function(dataset_id, tag) {
    read_parquet(file.path(base_path("matches", FALSE), dataset_id, paste0(tag, ".parquet")))
}
