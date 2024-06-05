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

write_data <- function(data_table, dataset, step, check_schema = TRUE, partitioning = "variable", key = "sensor_key") {
  table_path <- archive_path(dataset, "data", step)
  if (!dir.exists(dirname(table_path))) {
    dir.create(dirname(table_path), recursive = TRUE)
  }
  if (check_schema) {
    data_table <- data_table |>
      as_arrow_table2(schema = data_schema)
  }
  data_table |>
    arrange(!!sym(key), variable, date) |>
    write_dataset(table_path, partitioning = partitioning)
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
    write_dataset(table_path)
}

write_extra_metadata <- function(extra_table, dataset_name, conn) {
  path <- paste0(archive_path(dataset_name, "extra", "metadata"), ".parquet")
  if (!dir.exists(dirname(path))) {
    dir.create(dirname(path), recursive = TRUE)
  }
  write_parquet(extra_table, path)
}


write_series_groups <- function(series_groups, dataset, metadata) {
  metadata <- metadata |> select(key, sensor_key, dataset)
  series_groups <- series_groups |>
    left_join(metadata, by = "key") |>
    select(!c(key)) |>
    mutate(from = "raw")
  path <- paste0(archive_path(dataset, "extra", "series_groups"), ".parquet")
  if (!dir.exists(dirname(path))) {
    dir.create(dirname(path), recursive = TRUE)
  }
  write_parquet(series_groups, path)
}
