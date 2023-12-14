library(arrow, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

data_schema <- schema(
    station_id = utf8(),
    variable = utf8(),
    date = date32(),
    value = double(),
    merged = bool()
)
station_schema <- schema(
    dataset_id = utf8(),
    original_id = utf8(),
    station_id = utf8(),
    station_name = utf8(),
    network = utf8(),
    state = utf8(),
    lon = double(),
    lat = double(),
    elevation = double()
)
series_schema <- schema(
    series_id = utf8(),
    station_id = utf8(),
    variable = utf8(),
    qc_step = uint32(), # There's a bug when using uint8() here affecting concat_tables
    # merged_from = list_of(utf8())
)

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


# name_series <- function(full_table, dataset_id) {
#     full_table |>
#         mutate(
#             series_id = str_glue("/{dataset_id}/{station_id}/{qc_step}/{variable}") |> sapply(hash) |> unname()
#         )
# }

#' Gives a unique id to each station in the table.
#'
#' @param station_table An dataframe/Table containing the stations metadata.
#' @param dataset_id The id of the dataset to which the stations belong.
#'
#' @return A dataframe/Table containing the stations metadata with the station_id column added.
name_stations <- function(station_table) {
    station_table |>
        mutate(
            station_id = str_glue("/{dataset_id}/{original_id}") |> sapply(hash) |> unname(),
        )
}

# split_data_metadata <- function(full_table) {
#     list(
#         select(full_table, all_of(data_schema$names)) |> as_arrow_table2(data_schema),
#         select(full_table, all_of(series_schema$names)) |> distinct() |> as_arrow_table2(series_schema)
#     )
# }

split_data_metadata <- function(full_table) {
    list(
        select(full_table, all_of(data_schema$names)) |> as_arrow_table2(data_schema),
        select(full_table, all_of(station_schema$names)) |> distinct() |> as_arrow_table2(station_schema)
    )
}

split_station_metadata <- function(full_station_list) {
    base_meta <- select(full_station_list, all_of(station_schema$names)) |> as_arrow_table2(station_schema)
    extra_meta <- select(full_station_list, !all_of(station_schema$names), station_id)
    list("base" = base_meta, "extra" = extra_meta)
}

base_path <- function(section, provisional) {
    path <- file.path("db", section)
    if (provisional) {
        path <- file.path(path, "intermediate")
    }
    path
}
