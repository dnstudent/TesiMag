library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(stringi, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")
source("src/database/test.R")
source("src/database/definitions.R")
source("src/analysis/data/quality_check.R")

dataset_spec.aa.api <- function() {
    list(
        "https://data.civis.bz.it/it/dataset/misure-meteo-e-idrografiche/resource/8cc47a38-1a93-47bf-871d-07b49dce56d0",
        "regional",
        "OpenData dell''Alto Adige. Risoluzione suboraria. Dati aggregati manualmente."
    )
}

dataset_spec.aa.xlsx <- function() {
    list(
        "https://data.civis.bz.it/it/dataset/precipitazioni-giornaliere",
        "regional",
        "Aggregati giornalieri dell''Alto Adige forniti tramite fogli di calcolo. Risoluzione giornaliera."
    )
}

dataset_spec.aa <- function() {
    list(
        "locale",
        "regional",
        "Raccolta dei dataset Alto Adige API e Alto Adige XLSX. Risoluzione giornaliera."
    )
}

dataset_spec.tn <- function() {
    list(
        "http://storico.meteotrentino.it/web.htm",
        "regional",
        "Dati forniti da Meteotrentino. Risoluzione giornaliera e/o suboraria. Laddove mancante, l''aggregazione giornaliera è stata fatta manualmente dai dati."
    )
}

dataset_spec <- function() {
    list(
        "locale",
        "regional",
        "Raccolta dei dataset Alto Adige API, Alto Adige XLSX e Trentino. Risoluzione giornaliera."
    )
}

path_tre <- file.path(path.ds, "ARPA", "TRENTINO")
path_bz <- file.path(path_tre, "bolzano")
path_tn <- file.path(path_tre, "trento")

normalize_original <- function(original_id) {
    original_id |>
        str_to_upper() |>
        str_trim()
}

daily_aggregation <- function(data, threshold = 0.97) {
    data |>
        filter(!is.na(value)) |>
        gross_errors_check(value) |>
        filter(qc_gross) |>
        group_by(station_id, date = as.Date(time)) |>
        summarise(
            T_MIN = min(value),
            T_MAX = max(value),
            n_measures = n(),
            .groups = "drop"
        ) |>
        to_duckdb() |>
        group_by(station_id, year = year(date), month = month(date)) |>
        mutate(max_measures = max(n_measures, na.rm = TRUE)) |>
        ungroup() |>
        filter(n_measures / max_measures > threshold) |>
        select(!c(max_measures, n_measures, year, month)) |>
        pivot_longer(c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        compute()
}

load_bz_api_meta <- function() {
    # Temperature sensors
    sensor_codes <- read_csv_arrow(file.path(path_bz, "api", "sensors.csv")) |>
        filter(TYPE == "LT")

    sf::read_sf(file.path(path_bz, "api", "stations.geojson")) |>
        sf::st_drop_geometry() |>
        semi_join(sensor_codes, by = "SCODE") |>
        rename(id = SCODE, name = NAME_I, elevation = ALT, lon = LONG, lat = LAT) |>
        mutate(network = "bz_api", dataset = "Alto Adige API", id = normalize_original(id)) |>
        select(any_of(station_schema$names)) |>
        as_arrow_table()
}

load_bz_xlsx_meta <- function() {
    read_csv_arrow(file.path(path_bz, "xlsx", "meta.csv")) |>
        mutate(
            station_name = station_name |>
                stri_trans_general("latin-ascii") |>
                str_squish() |>
                str_remove(" multiannual LT N daily temperature precipitation")
        ) |>
        sf::st_as_sf(coords = c("x", "y"), crs = "EPSG:25832", remove = FALSE) |>
        sf::st_transform("EPSG:4326") |>
        mutate(coords = as_tibble(sf::st_coordinates(geometry)), network = "bz_xlsx", elevation = as.double(elevation)) |>
        sf::st_drop_geometry() |>
        unnest_wider(coords) |>
        rename(lon = X, lat = Y, x_EPSG_25832 = x, y_EPSG_25832 = y, name = station_name, id = original_id) |>
        mutate(dataset = "Alto Adige XLSX", id = normalize_original(id))
}

load_data_bz <- function(first_date, last_date) {
    bz_api <- open_dataset(file.path(path_bz, "api", "raw_parquet")) |>
        rename(station_id = original_id, value = VALUE, time = DATE) |>
        mutate(station_id = cast(station_id, utf8()) |> str_to_upper() |> str_trim()) |>
        daily_aggregation(0.9) |>
        relocate(any_of(data_schema$names)) |>
        compute()

    bz_xlsx <- read_parquet(file.path(path_bz, "xlsx", "data.parquet"), as_data_frame = FALSE) |>
        rename(station_id = original_id) |>
        mutate(station_id = cast(station_id, utf8()) |> str_to_upper() |> str_trim()) |>
        select(-station_name) |>
        to_duckdb() |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        relocate(any_of(data_schema$names)) |>
        to_arrow() |>
        compute()

    concat_tables(
        bz_api |> anti_join(bz_xlsx, by = c("station_id", "variable", "date")) |> compute(),
        bz_xlsx,
        unify_schemas = FALSE
    ) |>
        filter(first_date <= date & date <= last_date) |>
        arrange(station_id, variable, date) |>
        compute()
}

load_bz <- function(first_date, last_date) {
    data <- load_data_bz(first_date, last_date)

    xlsx_meta <- load_bz_xlsx_meta()
    api_meta <- load_bz_api_meta() |>
        anti_join(xlsx_meta, by = "id") |>
        relocate(any_of(colnames(xlsx_meta))) |>
        collect()

    meta <- bind_rows(
        api_meta,
        xlsx_meta
    ) |> semi_join(data |> distinct(station_id) |> collect(), join_by(id == station_id))

    list("meta" = meta, "data" = data)
}

qc_aggregated <- function(data, value_var, valid_var) {
    select(data, station_id, date, value = {{ value_var }}, valid = {{ valid_var }}) |>
        filter(valid < 150L, valid != 140L, !is.na(value)) |> # http://storico.meteotrentino.it/
        gross_errors_check(value) |>
        arrange(station_id, date) |>
        group_by(station_id) |>
        collect() |>
        repeated_values_check() |>
        integer_streak_check(threshold = 8L) |>
        ungroup() |>
        filter(qc_gross & qc_repeated & qc_int_streak) |>
        select(!c(valid, starts_with("qc_"))) |>
        as_arrow_table()
}

load_data_tn <- function(first_date, last_date) {
    subh_data <- open_dataset(file.path(path_tn, "fragments_subhour", "dataset")) |>
        mutate(original_id = cast(original_id, utf8()) |> str_to_upper() |> str_trim()) |>
        rename(station_id = original_id) |>
        daily_aggregation(0.9) |>
        relocate(any_of(data_schema$names)) |>
        compute()

    given_aggregated <- read_parquet(file.path(path_tn, "fragments", "data.parquet"), as_data_frame = FALSE) |>
        filter(value_TMIN < value_TMAX) |>
        mutate(station_id = cast(original_id, utf8()), date = as.Date(time)) |>
        select(-time, -original_id) |>
        compute()

    given_remaining <- concat_tables(
        T_MIN = qc_aggregated(given_aggregated, value_TMIN, valid_TMIN) |> mutate(variable = "T_MIN") |> compute(),
        T_MAX = qc_aggregated(given_aggregated, value_TMAX, valid_TMAX) |> mutate(variable = "T_MAX") |> compute(),
        unify_schemas = FALSE
    )

    # return(list(given_remaining, subh_data))

    integrations <- anti_join(subh_data, given_remaining, by = c("station_id", "date", "variable")) |>
        relocate(all_of(colnames(given_remaining))) |>
        compute()

    concat_tables(
        given_remaining,
        integrations,
        unify_schemas = FALSE
    ) |>
        filter(first_date <= date & date <= last_date) |>
        mutate(dataset = "Trentino") |>
        relocate(all_of(data_schema$names)) |>
        compute()
}

load_meta_tn <- function() {
    vroom::vroom(file.path(path_tn, "meta.csv"), show_col_types = FALSE) |>
        mutate(across(c(inizio, fine), dmy), dataset = "Trentino", network = "meteotrentino", id = codice |> str_squish() |> str_to_upper()) |>
        select(-codice, -nomebreve) |>
        rename(name = nome, elevation = quota, lat = latitudine, lon = longitudine, east = est) |>
        as_tibble() |>
        as_arrow_table()
}

load_tn <- function(first_date, last_date) {
    data <- load_data_tn(first_date, last_date)
    meta <- load_meta_tn() |>
        semi_join(data, join_by(id == station_id)) |>
        compute()

    list("meta" = meta, "data" = data)
}

load_daily_data.taa <- function(first_date, last_date) {
    db1 <- load_bz(first_date, last_date)
    db2 <- load_tn(first_date, last_date)

    metas <- bind_rows(db1$meta |> collect(), db2$meta |> collect()) |> mutate(state = "Trentino-Alto Adige")
    datas <- concat_tables(
        db1$data |>
            mutate(dataset = "Alto Adige") |>
            relocate(all_of(data_schema$names)) |>
            compute(),
        db2$data,
        unify_schemas = FALSE
    )

    list("meta" = metas |> as_arrow_table(), "data" = datas)
}
