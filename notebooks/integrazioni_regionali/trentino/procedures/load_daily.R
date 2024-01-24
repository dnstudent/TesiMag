library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(stringi, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")
source("src/database/test.R")
source("src/database/definitions.R")
source("src/database/query/data.R")
source("src/analysis/data/quality_check.R")
source("src/analysis/data/aggregation.R")

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
        "Dati forniti da Meteotrentino. Risoluzione giornaliera e/o suboraria. Combinazione di dati da automatiche e da annali. Laddove mancante, l''aggregazione giornaliera è stata fatta manualmente dai dati."
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

load_bz_api_meta <- function() {
    # Temperature sensors
    sensor_codes <- read_csv_arrow(file.path(path_bz, "api", "sensors.csv")) |>
        filter(TYPE == "LT")

    sf::read_sf(file.path(path_bz, "api", "stations.geojson")) |>
        sf::st_drop_geometry() |>
        semi_join(sensor_codes, by = "SCODE") |>
        rename(original_id = SCODE, name = NAME_I, elevation = ALT, lon = LONG, lat = LAT) |>
        mutate(network = "bz_api", original_dataset = "Alto Adige API", original_id = normalize_original(original_id), kind = "automatiche") |>
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
        rename(lon = X, lat = Y, x_EPSG_25832 = x, y_EPSG_25832 = y, name = station_name) |>
        mutate(original_dataset = "Alto Adige XLSX", original_id = normalize_original(original_id), kind = "unknown") |>
        as_arrow_table()
}

load_data_bz <- function() {
    bz_api <- query_parquet(file.path(path_bz, "api", "dataset", "*", "*.parquet")) |>
        rename(station_id = original_id, value = VALUE, time = DATE) |>
        mutate(dataset = "Alto Adige API") |>
        daily_aggregation(quality_threshold = 0.8) |>
        select(-dataset) |>
        to_arrow() |>
        relocate(any_of(data_schema$names)) |>
        compute()

    bz_xlsx <- query_parquet(file.path(path_bz, "xlsx", "data.parquet")) |>
        rename(station_id = original_id) |>
        mutate(station_id = as.character(station_id) |> str_to_upper() |> str_trim()) |>
        select(-station_name) |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        relocate(any_of(data_schema$names)) |>
        compute()

    concat_tables(
        bz_api |> anti_join(bz_xlsx, by = c("station_id", "variable", "date")) |> compute(),
        bz_xlsx,
        unify_schemas = FALSE
    ) |>
        arrange(station_id, variable, date) |>
        compute()
}

load_meta_bz <- function() {
    xlsx_meta <- load_bz_xlsx_meta()
    api_meta <- load_bz_api_meta()

    common <- xlsx_meta |>
        select(!c(lon, lat, elevation, kind)) |>
        inner_join(api_meta |> select(!c(name, network, original_dataset)), by = "original_id") |>
        collect()
    extra <- api_meta |>
        anti_join(common, by = "original_id") |>
        collect()
    bind_rows(
        common,
        extra
    ) |> as_arrow_table()
}

load_bz <- function() {
    meta <- load_meta_bz()
    data <- load_data_bz()

    data <- data |>
        left_join(meta |> select(station_id = original_id, dataset = original_dataset), by = "station_id", relationship = "many_to_one") |>
        compute()

    list("meta" = meta, "data" = data)
}

load_data_tn <- function() {
    auto_data <- query_parquet(file.path(path_tn, "fragments", "t", "data.parquet")) |>
        filter(value_TMIN < value_TMAX) |>
        mutate(station_id = as.character(original_id)) |>
        select(station_id, date, T_MIN = value_TMIN, valid_TMIN, T_MAX = value_TMAX, valid_TMAX) |>
        filter(if_all(starts_with("valid"), ~ (. < 150L & . != 140L))) |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        filter(abs(value) < 50) |>
        select(!starts_with("valid")) |>
        compute()

    annali_data <- query_parquet(file.path(path_tn, "fragments", "annali.parquet"), auto_data$src$con) |>
        mutate(date = if_else(variable == "T_MIN", as.Date(time), as.Date(sql("time - INTERVAL 1 DAY")))) |>
        rename(station_id = original_id) |>
        select(station_id, date, variable, value) |>
        anti_join(auto_data, by = c("station_id", "date", "variable"))

    rows_append(auto_data, annali_data) |>
        mutate(dataset = "Trentino") |>
        to_arrow() |>
        relocate(all_of(data_schema$names)) |>
        compute()
}

load_meta_tn <- function() {
    vroom::vroom(file.path(path_tn, "meta.csv"), show_col_types = FALSE) |>
        as_tibble() |>
        mutate(across(c(inizio, fine), dmy), original_dataset = "Trentino", network = "meteotrentino", original_id = codice |> str_squish() |> str_to_upper(), kind = "unknown") |>
        select(-codice, -nomebreve) |>
        rename(name = nome, elevation = quota, lat = latitudine, lon = longitudine, east = est) |>
        as_arrow_table()
}

load_tn <- function() {
    data <- load_data_tn()
    meta <- load_meta_tn()

    # Filtering out stations that do not provide temperatures
    meta <- meta |>
        semi_join(data, by = c("original_id" = "station_id")) |>
        compute()

    list("meta" = meta, "data" = data)
}

load_daily_data.taa <- function() {
    db1 <- load_bz()
    db2 <- load_tn()

    metas <- bind_rows(db1$meta |> collect(), db2$meta |> collect()) |> mutate(state = "Trentino-Alto Adige")
    datas <- concat_tables(
        db1$data |>
            relocate(all_of(data_schema$names)) |>
            compute(),
        db2$data,
        unify_schemas = FALSE
    )

    list("meta" = metas |> as_arrow_table(), "data" = datas)
}
