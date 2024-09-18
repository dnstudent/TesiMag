library(arrow, warn.conflicts = F)
library(dplyr, warn.conflicts = F)
library(sf, warn.conflicts = F)

source("src/paths/paths.R")
source("src/load/tools.R")

dataset_spec <- function() {
    list(
        "https://meteo.data.gouv.fr/datasets/6569b51ae64326786e4e8e1a",
        "national",
        "Dataset di MeteoFrance, stazioni di rilevazione storiche e moderne francesi. Metadati completi qui: https://donneespubliques.meteofrance.fr/?fond=contenu&id_contenu=37"
    )
}

load_meta <- function(ita_bounds) {
    from_data <- open_dataset(fs::path(path.ds, "MeteoFrance", "dataset")) |>
        select(series_id = NUM_POSTE, name = NOM_USUEL, lat = LAT, lon = LON, elevation = ALTI) |>
        distinct() |>
        collect()

    meta_path <- fs::path(path.ds, "MeteoFrance", "stations.geojson")

    from_meta <- read_sf(meta_path) |>
        st_drop_geometry() |>
        rename(name = NOM_USUEL, lat = LAT_DG, lon = LON_DG, elevation = ALTI, series_id = NUM_POSTE, town = COMMUNE)

    geometa <- full_join(from_data, from_meta, by = "series_id") |>
        mutate(
            name = coalesce(name.x, name.y),
            lat = coalesce(lat.x, lat.y),
            lon = coalesce(lon.x, lon.y),
            elevation = as.numeric(coalesce(elevation.x, elevation.y)),
            dataset = "MeteoFrance",
            user_code = series_id,
            network = "MeteoFrance",
            sensor_id = NA_character_,
            station_id = NA_character_,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            district = NA_character_,
            province = NA_character_,
            country = "France",
            kind = "mixed",
        ) |>
        select(-ends_with(".x"), -ends_with(".y")) |>
        st_md_to_sf()

    close_stations <- geometa |>
        st_filter(ita_bounds, .predicate = st_is_within_distance, dist = units::set_units(200, "km"))

    excluded_but_close <- geometa |>
        anti_join(close_stations |> st_drop_geometry(), by = "station_id") |>
        st_filter(close_stations, .predicate = st_is_within_distance, dist = units::set_units(500, "m")) |>
        st_drop_geometry()

    bind_rows(close_stations |> st_drop_geometry(), excluded_but_close) |> as_arrow_table()
}

load_data <- function(meta) {
    data_dir <- fs::path(path.ds, "MeteoFrance", "dataset")
    t_max <- open_dataset(data_dir) |>
        semi_join(meta, by = c("NUM_POSTE" = "series_id")) |>
        filter(!is.na(TX), !is.na(QTX), QTX > 0L) |>
        select(series_id = NUM_POSTE, date = AAAAMMJJ, value = TX) |>
        mutate(date = lubridate::ymd(date))

    t_min <- open_dataset(data_dir) |>
        semi_join(meta, by = c("NUM_POSTE" = "series_id")) |>
        filter(!is.na(TN), !is.na(QTN), QTN > 0L) |>
        select(series_id = NUM_POSTE, date = AAAAMMJJ, value = TN) |>
        mutate(date = lubridate::ymd(date))

    concat_tables(
        t_max |> mutate(variable = "T_MAX") |> compute(),
        t_min |> mutate(variable = "T_MIN") |> compute()
    ) |>
        mutate(dataset = "MeteoFrance") |>
        compute()
}

load_daily_data.meteofrance <- function(ita_bounds) {
    meta <- load_meta(ita_bounds)
    data <- load_data(meta)
    list(meta = meta, data = data)
}
