library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(rlang, warn.conflicts = FALSE)

data_schema <- schema(
    series_id = utf8(),
    date = date32(),
    value = float(),
    merged = bool()
)
station_schema <- schema(
    station_id = utf8(),
    station_name = utf8(),
    network = utf8(),
    state = utf8(),
    lon = double(),
    lat = double(),
    elevation = double(),
    dataset_id = utf8(),
    original_id = utf8()
)
series_schema <- schema(
    series_id = utf8(),
    station_id = utf8(),
    variable = utf8(),
    qc_step = uint32(), # There's a bug when using uint8() here affecting concat_tables
    # merged_from = list_of(utf8())
)

as_arrow_table2 <- function(table, schema) {
    table |>
        as_tibble() |>
        select(all_of(schema$names)) |>
        as_arrow_table(schema = schema)
}

name_series <- function(full_table, dataset_id) {
    full_table |>
        mutate(
            series_id = str_glue("/{dataset_id}/{station_id}/{qc_step}/{variable}") |> sapply(hash) |> unname()
        )
}

split_data_metadata <- function(full_table) {
    list(
        select(full_table, all_of(data_schema$names)) |> as_arrow_table2(data_schema),
        select(full_table, all_of(series_schema$names)) |> distinct() |> as_arrow_table2(series_schema)
    )
}

write_data <- function(data_table, dataset_id) {
    data_table |>
        as_arrow_table2(schema = data_schema) |>
        arrange(series_id, date) |>
        write_parquet(file.path("db", "data", paste0(dataset_id, ".parquet")))
}

write_station_metadata <- function(station_table, dataset_id, auto_complete = TRUE) {
    station_table <- collect(station_table)
    if (auto_complete) {
        station_table <- station_table |>
            mutate(
                station_id = str_glue("/{dataset_id}/{original_id}") |> sapply(hash) |> unname(),
                dataset_id = dataset_id
            )
    }
    station_table |>
        assert(is_uniq, station_id) |>
        as_arrow_table2(schema = station_schema) |>
        write_parquet(file.path("db", "metadata", "stations", paste0(dataset_id, ".parquet")))
    extra_meta_table <- select(station_table, !all_of(station_schema$names), station_id) |> as_tibble()
    # add_column(station_id = main_table$station_id)
    extra_meta_table |>
        write_parquet(file.path("db", "metadata", "extra", paste0(dataset_id, ".parquet")))
}

write_series_metadata <- function(series_table, dataset_id, auto_complete = TRUE) {
    series_table <- collect(series_table)
    if (auto_complete) {
        series_table <- name_series(series_table, dataset_id)
    }
    series_table |>
        assert(is_uniq, series_id) |>
        as_arrow_table2(schema = series_schema) |>
        write_parquet(file.path("db", "metadata", "series", paste0(dataset_id, ".parquet")))
}

# write_merged_data <- function(merged_data) {
#     merged_data |>
#         as_arrow_table2(schema = data_schema) |>
# }
