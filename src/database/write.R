library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(rlang, warn.conflicts = FALSE)
library(fs, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

source("src/database/definitions.R")
source("src/database/tools.R")

write_data <- function(data_table, dataset, step) {
    table_path <- archive_path(dataset, "data", step)
    if (!dir.exists(dirname(table_path))) {
        dir.create(dirname(table_path), recursive = TRUE)
    }
    data_table |>
        as_arrow_table2(schema = data_schema) |>
        arrange(station_id, variable, date) |>
        write_parquet(table_path)
}

write_metadata <- function(metadata_table, dataset, step) {
    table_path <- archive_path(dataset, "metadata", step)
    if (!dir.exists(dirname(table_path))) {
        dir.create(dirname(table_path), recursive = TRUE)
    }
    metadata_table |>
        as_arrow_table2(schema = station_schema) |>
        write_parquet(table_path)
}

write_extra_metadata <- function(extra_table, dataset_name, conn) {
    dbWriteTable(conn, paste0("extra_", dataset_name), extra_table, overwrite = TRUE)
}
