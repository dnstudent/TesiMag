library(arrow, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(rlang, warn.conflicts = FALSE)

source("src/database/definitions.R")

as_arrow_table2.data.frame <- function(table, schema) {
    table |>
        as_tibble() |>
        select(all_of(schema$names)) |>
        as_arrow_table(schema = schema)
}

as_arrow_table2.ArrowObject <- function(table, schema) {
    table <- table |>
        select(all_of(schema$names)) |>
        compute()
    table$cast(schema)
}

as_arrow_table2.arrow_dplyr_query <- function(table, schema) {
    table <- table |>
        select(all_of(schema$names)) |>
        compute()
    table$cast(schema)
}

as_arrow_table2 <- function(x, ...) UseMethod("as_arrow_table2", x)

#' Gives a unique id to each station in the table.
#'
#' @param station_table An dataframe/Table containing the stations metadata.
#' @param dataset_id The id of the dataset to which the stations belong.
#'
#' @return A dataframe/Table containing the stations metadata with the station_id column added.
# name_stations <- function(station_table) {
#     station_table |>
#         mutate(
#             station_id = str_glue("/{dataset}/{original_id}") |> sapply(hash) |> unname(),
#         )
# }

split_data_metadata <- function(full_table) {
    list(
        select(full_table, all_of(data_schema$names)) |> as_arrow_table2(data_schema),
        select(full_table, all_of(station_schema$names)) |> distinct() |> as_arrow_table2(station_schema)
    )
}

split_station_metadata <- function(full_station_list) {
    base_meta <- select(full_station_list, all_of(station_schema$names)) |> as_arrow_table2(station_schema)
    extra_meta <- select(full_station_list, !all_of(station_schema$names), original_dataset, original_id) |> as_arrow_table()
    list("base" = base_meta, "extra" = extra_meta)
}

base_path <- function(section, provisional) {
    path <- file.path("db", section)
    if (provisional) {
        path <- file.path(path, "intermediate")
    }
    path
}

as_database.data.frame <- function(meta, data) {
    list(
        "meta" = as_arrow_table2(meta, station_schema),
        "data" = if ("Dataset" %in% class(data) || "arrow_dplyr_query" %in% class(data)) {
            data
        } else {
            as_arrow_table2(data, data_schema)
        }
    )
}

as_database.ArrowTabular <- function(meta, data) {
    as_database.data.frame(meta, data)
}

as_database.list <- function(database) {
    if ("Table" %in% class(database$meta) && "ArrowObject" %in% class(database$data)) {
        database
    } else {
        as_database.ArrowTabular(database$meta, database$data)
    }
}

as_database <- function(x, ...) UseMethod("as_database", x)

gap_fill_database <- function(database, start_date, stop_date) {
    database$data <- database$data |>
        collect() |>
        as_tsibble(key = c(dataset, station_id, variable), index = date) |>
        fill_gaps(.full = TRUE, .start = start_date, .end = stop_date) |>
        as_tibble() |>
        arrange(dataset, station_id, variable, date) |>
        as_arrow_table2(data_schema)
    database
}

filter_inside <- function(metadata, shape) {
    metadata |>
        collect() |>
        st_md_to_sf(remove = FALSE) |>
        st_filter(shape, .predicate = st_within) |>
        st_drop_geometry()
}
