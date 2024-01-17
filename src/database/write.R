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

# data_schema <- schema(
#     series_id = utf8(),
#     date = date32(),
#     value = float(),
#     merged = bool()
# )

write_data <- function(data_table, dataset_id, tag, provisional) {
    table_path <- file.path(base_path("data", provisional), dataset_id, paste0(tag, ".parquet"))
    if (!dir.exists(dirname(table_path))) {
        dir.create(dirname(table_path), recursive = TRUE)
    }
    data_table |>
        as_arrow_table2(schema = data_schema) |>
        arrange(station_id, variable, date) |>
        write_parquet(table_path)
}

write_metadata <- function(metadata_table, dataset_id, tag, provisional) {
    table_path <- file.path(base_path("metadata", provisional), dataset_id, paste0(tag, ".parquet"))
    if (!dir.exists(dirname(table_path))) {
        dir.create(dirname(table_path), recursive = TRUE)
    }
    metadata_table |>
        as_arrow_table2(schema = station_schema) |>
        write_parquet(table_path)
}

# write_extra_metadata <- function(extra_table, dataset_id, provisional) {
#     table_path <- file.path(base_path("metadata", provisional), dataset_id, paste0("extra.parquet"))
#     if (!dir.exists(dirname(table_path))) {
#         dir.create(dirname(table_path), recursive = TRUE)
#     }
#     extra_table |>
#         write_parquet(table_path)
# }

write_extra_metadata <- function(extra_table, dataset_name, conn) {
    dbWriteTable(conn, paste0("extra_", dataset_name), extra_table, overwrite = TRUE)
}

# write_station_metadata <- function(station_table, dataset_id, auto_complete, provisional) {
#     station_table <- collect(station_table)
#     if (auto_complete) {
#         station_table <- station_table |>
#             mutate(
#                 dataset_id = dataset_id
#             ) |>
#             name_stations()
#     }
#     station_table |>
#         assert(is_uniq, station_id) |>
#         as_arrow_table2(schema = station_schema) |>
#         write_parquet(file.path(base_path("metadata", provisional), "stations", paste0(dataset_id, ".parquet")))
#     extra_meta_table <- select(station_table, !all_of(station_schema$names), station_id) |> as_tibble()
#     # add_column(station_id = main_table$station_id)
#     extra_meta_table |>
#         write_parquet(file.path(base_path("metadata", provisional), "extra", paste0(dataset_id, ".parquet")))
# }

# write_series_metadata <- function(series_table, dataset_id, auto_complete, provisional) {
#     series_table <- collect(series_table)
#     if (auto_complete) {
#         series_table <- name_series(series_table, dataset_id)
#     }
#     series_table |>
#         assert(is_uniq, series_id) |>
#         as_arrow_table2(schema = series_schema) |>
#         write_parquet(file.path(base_path("metadata", provisional), "series", paste0(dataset_id, ".parquet")))
# }

# write_merged_data <- function(merged_data) {
#     merged_data |>
#         as_arrow_table2(schema = data_schema) |>
# }
