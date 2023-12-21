library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(stringi, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")
source("src/analysis/data/quality_check.R")

path_tre <- file.path(path.ds, "ARPA", "TRENTINO")
path_bz <- file.path(path_tre, "bolzano")
path_tn <- file.path(path_tre, "trento")

summarise_daily <- function(path_from, path_to) {
    open_dataset(path_from) |>
        rename(time = DATE, value = VALUE) |>
        mutate(original_id = cast(original_id, utf8())) |>
        gross_errors_check(value) |>
        group_by(original_id, date = as.Date(time)) |>
        summarise(
            T_MIN = min(value, na.rm = TRUE),
            T_MAX = max(value, na.rm = TRUE),
            any_gross = any(qc_gross, na.rm = TRUE),
            .groups = "drop"
        ) |>
        write_dataset(path_to)
}

load_bz_api_meta <- function() {
    sensor_codes <- read_csv_arrow(file.path(path_bz, "api", "sensors.csv")) |>
        filter(TYPE == "LT")
    sf::read_sf(file.path(path_bz, "api", "stations.geojson")) |>
        sf::st_drop_geometry() |>
        inner_join(sensor_codes, by = "SCODE") |>
        rename(original_id = SCODE, station_name = NAME_I, elevation = ALT, lon = LONG, lat = LAT) |>
        mutate(dataset_id = "TAA", network = "bz_api") |>
        name_stations() |>
        as_arrow_table()
}

load_bz_xlsx_meta <- function() {
    meta <- read_csv_arrow(file.path(path_bz, "xlsx", "meta.csv")) |>
        mutate(
            station_name = station_name |>
                stri_trans_general("latin-ascii") |>
                str_squish() |>
                str_remove(" multiannual LT N daily temperature precipitation")
        )

    meta |>
        sf::st_as_sf(coords = c("x", "y"), crs = "EPSG:25832", remove = FALSE) |>
        sf::st_transform("EPSG:4326") |>
        mutate(coords = as_tibble(sf::st_coordinates(geometry)), dataset_id = "TAA", network = "bz_xlsx", elevation = as.double(elevation)) |>
        unnest_wider(coords) |>
        rename(lon = X, lat = Y, x_EPSG_25832 = x, y_EPSG_25832 = y) |>
        sf::st_drop_geometry() |>
        select(-geometry) |>
        name_stations() |>
        as_arrow_table()
}

load_data_bz <- function() {
    bz_api <- open_dataset(file.path(path_bz, "api", "dataset")) |>
        filter(!any_gross) |>
        select(-any_gross) |>
        inner_join(load_bz_api_meta() |> select(station_id, original_id), by = "original_id") |>
        select(-original_id) |>
        compute()

    bz_xlsx <- read_parquet(file.path(path_bz, "xlsx", "data.parquet"), as_data_frame = FALSE) |>
        select(-station_name) |>
        filter(date >= as.Date("2000-01-01")) |>
        mutate(original_id = cast(original_id, utf8())) |>
        relocate(original_id, date, T_MIN, T_MAX) |>
        inner_join(load_bz_xlsx_meta() |> select(station_id, original_id), by = "original_id") |>
        select(-original_id) |>
        compute()

    concat_tables(bz_api, bz_xlsx, unify_schemas = FALSE) |>
        collect() |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        mutate(merged = FALSE) |>
        as_arrow_table2(data_schema)
}

qc_given_tn <- function(data, value_var, valid_var) {
    select(data, original_id, date, value = {{ value_var }}, valid = {{ valid_var }}) |>
        filter(valid < 150) |> # http://storico.meteotrentino.it/
        gross_errors_check(value) |>
        arrange(original_id, date) |>
        group_by(original_id) |>
        collect() |>
        repeated_values_check() |>
        integer_streak_check(threshold = 8L) |>
        filter(!(qc_gross | qc_repeated | qc_int_streak)) |>
        select(!starts_with("qc_")) |>
        ungroup() |>
        as_arrow_table()
}

load_data_tn <- function() {
    subh_data <- open_dataset(file.path(path_tn, "aggregated")) |>
        filter(!any_gross) |>
        select(-any_gross) |>
        collect() |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        as_arrow_table()

    given_data <- read_parquet(file.path(path_tn, "fragments", "data.parquet"), as_data_frame = FALSE) |>
        filter(value_TMIN < value_TMAX) |>
        mutate(date = as.Date(time), original_id = cast(original_id, utf8())) |>
        select(-time)

    ## T_MIN
    given_remaining <- concat_tables(
        T_MIN = qc_given_tn(given_data, value_TMIN, valid_TMIN) |> mutate(variable = "T_MIN") |> compute(),
        T_MAX = qc_given_tn(given_data, value_TMAX, valid_TMAX) |> mutate(variable = "T_MAX") |> compute(),
        unify_schemas = FALSE
    ) |>
        select(-valid) |>
        compute()

    concat_tables(
        given_remaining,
        anti_join(subh_data, given_remaining, by = c("original_id", "date", "variable")) |>
            relocate(all_of(colnames(given_remaining))) |>
            compute(),
        unify_schemas = FALSE
    )
}

load_meta_tn <- function() {
    vroom::vroom(file.path(path_tn, "meta.csv"), show_col_types = FALSE) |>
        mutate(across(c(inizio, fine), dmy)) |>
        rename(station_name = nome, original_id = codice, elevation = quota, lat = latitudine, lon = longitudine, east = est) |>
        as_arrow_table()
}
