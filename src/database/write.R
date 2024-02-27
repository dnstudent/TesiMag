library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(rlang, warn.conflicts = FALSE)
library(fs, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

source("src/database/data_model.R")
source("src/database/tools.R")

write_data <- function(data_table, dataset, step, check_schema = TRUE) {
    table_path <- archive_path(dataset, "data", step)
    if (!dir.exists(dirname(table_path))) {
        dir.create(dirname(table_path), recursive = TRUE)
    }
    if (check_schema) {
        data_table <- data_table |>
            as_arrow_table2(schema = data_schema)
    }
    data_table |>
        arrange(sensor_key, variable, date) |>
        write_parquet(table_path)
}

write_metadata <- function(metadata_table, dataset, step, check_schema = TRUE) {
    table_path <- archive_path(dataset, "metadata", step)
    if (!dir.exists(dirname(table_path))) {
        dir.create(dirname(table_path), recursive = TRUE)
    }
    if (check_schema) {
        metadata_table <- metadata_table |>
            as_arrow_table2(schema = meta_schema)
    }
    metadata_table |>
        write_parquet(table_path)
}

write_extra_metadata <- function(extra_table, dataset_name, conn) {
    dbWriteTable(conn, paste0("extra_", dataset_name), extra_table, overwrite = TRUE)
}

write_correction_coefficients <- function(correction_table, dataset, conn, metadata) {
    metadata <- metadata |> select(key, sensor_key, dataset)
    correction_table <- correction_table |>
        left_join(metadata, by = c("key_x" = "key")) |>
        left_join(metadata, by = c("key_y" = "key"), suffix = c("_x", "_y")) |>
        select(!c(key_x, key_y))
    dbWriteTable(conn, paste0("correction_", dataset), correction_table, overwrite = TRUE)
}
