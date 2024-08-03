library(vroom, warn.conflicts = F)
library(arrow, warn.conflicts = F)
library(lubridate, warn.conflicts = F)

source("src/paths/paths.R")

dataset_spec <- function() {
    list(
        "https://opendata.swiss/it/dataset/homogene-monatsdaten",
        "national",
        "Dataset MeteoSwiss"
    )
}

load_meta <- function() {
    meta_path <- fs::path(path.ds, "MeteoSwiss", "NBNC-daily", "liste-download-nbcn-d.csv")

    vroom(meta_path,
        delim = ";",
        locale = locale(encoding = "iso-8859-1"),
        col_types = cols(
            `Data since` = col_date(format = "%d.%m.%Y"),
            `Station height m. a. sea level` = col_integer(),
            CoordinatesE = col_integer(),
            CoordinatesN = col_integer(),
            Latitude = col_double(),
            Longitude = col_double(),
            .default = col_character()
        ),
        n_max = 29L
    ) |>
        mutate(
            dataset = "NBNC",
            sensor_id = NA_character_,
            station_id = NA_character_,
            series_id = `WIGOS-ID`,
            user_code = `station/location`,
            name = Station,
            elevation = as.numeric(elevation),
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            town = NA_character_,
            district = Canton,
            province = NA_character_,
            country = "The Swiss Confederation",
            kind = "unknown",
        ) |>
        select(-Abbreviation, -Station, -Canton)
}

load_fragment <- function(path) {

}


load_data <- function() {
    data_dir <- fs::path(path.ds, "MeteoSwiss", "NBNC-omogeneizzati", "fragments")
    open_csv_dataset(data_dir,
        unify_schemas = TRUE
    ) |>
        select(series_id = station_code, time = measure_date, value = TA_30MIN_MEAN) |>
        filter(!is.na(value)) |>
        group_by(series_id, date = lubridate::date(time)) |>
        summarise(T_MIN = min(value, na.rm = TRUE), T_MAX = max(value, na.rm = TRUE), n = n(), .groups = "drop") |>
        filter(n >= 44L) |>
        select(-n) |>
        compute() |>
        to_duckdb() |>
        tidyr::pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        mutate(dataset = "WSL") |>
        compute()
}

load_daily_data.wsl <- function() {
    list(meta = load_meta(), data = load_data())
}
