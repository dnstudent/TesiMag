library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)

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
    qc_step = utf8(),
    merged_from = list_of(utf8())
)

write_data <- function(data_table, dataset_id) {
    data_table |>
        relocate(all_of(data_schema$names)) |>
        as_tibble() |>
        as_arrow_table(schema = data_schema) |>
        arrange(series_id, date) |>
        write_parquet(file.path("db", "data", paste0(dataset_id, ".parquet")))
}

write_station_metadata <- function(station_table, dataset_id, auto_complete = TRUE) {
    if (auto_complete) {
        station_table <- station_table |>
            mutate(
                station_id = str_glue("/{dataset_id}/{original_id}") |> sapply(hash) |> unname(),
                dataset_id = dataset_id
            )
    }
    main_table <- select(station_table, all_of(station_schema$names)) |>
        relocate(
            all_of(station_schema$names)
        ) |>
        assert(is_uniq, station_id)
    main_table |>
        as_tsibble() |>
        as_arrow_table(schema = station_schema) |>
        write_parquet(file.path("db", "metadata", "stations", paste0(dataset_id, ".parquet")))
    extra_meta_table <- select(station_table, !all_of(station_schema$names)) |>
        add_column(station_id = main_table$station_id)
    extra_meta_table |>
        as_tibble() |>
        write_parquet(file.path("db", "metadata", "extra", paste0(dataset_id, ".parquet")))
}

write_series_metadata <- function(series_table, dataset_id, auto_complete = TRUE) {
    if (auto_complete) {
        series_table <- series_table |>
            mutate(
                series_id = str_glue("/{dataset_id}/{station_id}/{qc_step}/{variable}") |> sapply(hash) |> unname()
            )
    }
    series_table |>
        relocate(
            all_of(series_schema$names)
        ) |>
        assert(is_uniq, series_id) |>
        as_tibble() |>
        as_arrow_table(schema = series_schema) |>
        write_parquet(file.path("db", "metadata", "series", paste0(dataset_id, ".parquet")))
}
