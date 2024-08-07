library(arrow, warn.conflicts = F)
library(dplyr, warn.conflicts = F)
library(sf, warn.conflicts = F)
library(vroom, warn.conflicts = F)

source("src/paths/paths.R")
source("src/load/tools.R")

dataset_spec <- function() {
    list(
        "https://data.hub.geosphere.at/dataset/klima-v2-1d",
        "national",
        "Dataset di GeoSphere Austria, stazioni di rilevazione rete TAWES. Le aggregazioni sono prese dalle 18:10 del giorno precedente alle 18:00 del giorno indicato su intervalli di 10 min.",
        "https://doi.org/10.60669/gs6w-jd70"
    )
}

load_meta <- function(ita_bounds) {
    meta <- vroom(fs::path(path.ds, "GeoSphereAustria", "stations.csv"), col_types = cols(
        lat = col_double(),
        lon = col_double(),
        altitude = col_double(),
        valid_from = col_datetime(),
        valid_to = col_datetime(),
        has_sunshine = col_logical(),
        has_global_radiation = col_logical(),
        is_active = col_logical(),
        .default = col_character()
    )) |>
        as_tibble() |>
        rename(series_id = group_id, station_id = id, elevation = altitude, district = state) |>
        mutate(
            series_id = coalesce(series_id, station_id),
            valid_from = as.Date(valid_from),
            valid_to = as.Date(valid_to),
            dataset = "GeoSphereAustria",
            user_code = series_id,
            network = "TAWES",
            sensor_id = NA_character_,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            province = NA_character_,
            town = NA_character_,
            kind = "unknown"
        ) |>
        filter(type == "INDIVIDUAL")

    close_stations <- meta |>
        st_md_to_sf() |>
        st_filter(ita_bounds, .predicate = st_is_within_distance, dist = units::set_units(200, "km")) |>
        st_drop_geometry()

    close_stations |> bind_rows(meta |> semi_join(close_stations, by = "series_id") |> anti_join(close_stations, by = "station_id"))
}

load_data <- function(meta, dataconn) {
    data_dir <- fs::path(fs::path(path.ds, "GeoSphereAustria", "fragments", "tlmax.tlmax_flag.tlmin.tlmin_flag"))
    ds_schema <- schema(time = timestamp(unit = "us"), station_id = utf8(), tlmin = float64(), tlmin_flag = float64(), tlmax = float64(), tlmax_flag = float64(), substation = utf8())
    open_csv_dataset(data_dir,
        convert_options = csv_convert_options(include_missing_columns = TRUE, col_types = ds_schema),
        timestamp_parsers = "%Y-%m-%dT%H:%M:%OS"
    ) |>
        semi_join(meta, by = "station_id") |>
        select(station_id, time, T_MIN = tlmin, T_MAX = tlmax) |>
        mutate(date = as.Date(time)) |>
        select(-time) |>
        to_duckdb(con = dataconn) |>
        tidyr::pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        mutate(dataset = "GeoSphereAustria") |>
        filter(!is.na(value)) |>
        compute()
}

load_daily_data.geosphereaustria <- function(ita_bounds, dataconn) {
    meta <- load_meta(ita_bounds)
    list(meta = meta, data = load_data(meta, dataconn))
}
