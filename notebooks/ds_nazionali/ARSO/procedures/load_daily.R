library(arrow, warn.conflicts = F)
library(dplyr, warn.conflicts = F)
library(sf, warn.conflicts = F)
library(vroom, warn.conflicts = F)

source("src/paths/paths.R")
source("src/load/tools.R")

dataset_spec <- function() {
    list(
        "https://meteo.arso.gov.si/met/sl/archive/",
        "national",
        "Dataset del ministero dell'Ambiente Sloveno. Varie reti di misura (climatologiche, automatiche, \"principali\")."
    )
}

load_meta <- function(ita_bounds) {
    meta <- vroom(fs::path(path.ds, "meteo.si", "stations.csv"), col_types = cols(
        station_id = col_integer(),
        name = col_character(),
        lon = col_double(),
        lat = col_double(),
        elevation = col_double(),
        type = col_integer(),
        kind = col_character()
    ), locale = locale(encoding = "utf-8")) |>
        as_tibble() |>
        mutate(
            dataset = "ARSO",
            user_code = as.character(station_id),
            network = kind,
            sensor_id = NA_character_,
            series_id = as.character(station_id),
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            province = NA_character_,
            town = NA_character_,
            country = "Slovenia"
        )

    geometa <- meta |>
        st_md_to_sf()
    close_stations <- geometa |>
        st_filter(ita_bounds, .predicate = st_is_within_distance, dist = units::set_units(200, "km"))

    excluded_but_close <- geometa |>
        anti_join(close_stations |> st_drop_geometry(), by = "station_id") |>
        st_filter(close_stations, .predicate = st_is_within_distance, dist = units::set_units(500, "m")) |>
        st_drop_geometry()

    bind_rows(close_stations |> st_drop_geometry(), excluded_but_close)
}

load_data.trad <- function(meta, dataconn) {
    # CONTROLLATO: OK
    open_dataset(fs::path(path.ds, "meteo.si", "trad_ds.arrow"), format = "arrow") |>
        semi_join(meta |> filter(type != 4L), by = "station_id") |>
        rename(T_MIN = tmin, T_MAX = tmax) |>
        filter(T_MIN < T_MAX) |> # Ci sono due date in cui non è vero
        mutate(date = as.Date(time)) |>
        select(-time) |>
        to_duckdb(con = dataconn) |>
        pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
        filter(!is.na(value)) |>
        to_arrow() |>
        relocate(station_id, date, variable, value) |>
        compute()
}

load_data.auto <- function(meta, dataconn) {
    # CONTROLLATO: OK
    open_dataset(fs::path(path.ds, "meteo.si", "auto_ds.arrow"), format = "arrow") |>
        semi_join(meta |> filter(type == 4L), by = "station_id") |>
        mutate(time = lubridate::force_tz(time, tzone = "UTC") |> lubridate::with_tz("CET") - as.difftime(10, "mins")) |>
        compute() |>
        to_duckdb(con = conns$data) |>
        pivot_longer(cols = c("t2mmin", "t2mmax")) |>
        filter(!is.na(value)) |>
        # Mixing tmin and tmax; this should deal with mixups without compromising the min/max statistics
        group_by(station_id, date = as.Date(time), hour = hour(time)) |>
        summarise(tminh = min(value, na.rm = T), tmaxh = max(value, na.rm = T), .groups = "drop_last") |>
        #  Filtering out days having data covering less than 22 hours
        filter(sum(as.integer(!is.na(tminh)), na.rm = T) > 22L) |>
        summarise(T_MIN = min(tminh, na.rm = T), T_MAX = max(tmaxh, na.rm = T), .groups = "drop") |>
        pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        relocate(station_id, date, variable, value) |>
        compute()
}

load_data <- function(meta, dataconn) {
    concat_tables(
        load_data.trad(meta, dataconn),
        load_data.auto(meta, dataconn),
        unify_schemas = FALSE
    ) |>
        mutate(dataset = "ARSO", station_id = as.character(station_id)) |>
        compute()
}

load_daily_data.arso <- function(ita_bounds, dataconn) {
    meta <- load_meta(ita_bounds)
    list(meta = meta |> mutate(station_id = as.character(station_id)), data = load_data(meta, dataconn))
}
