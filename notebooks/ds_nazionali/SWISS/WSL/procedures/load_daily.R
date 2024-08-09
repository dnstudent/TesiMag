library(vroom, warn.conflicts = F)
library(arrow, warn.conflicts = F)
library(lubridate, warn.conflicts = F)

source("src/paths/paths.R")
source("notebooks/ds_nazionali/SWISS/procedures/load_daily.R")

dataset_spec <- function() {
    list(
        "https://www.wsl.ch/it/services-produkte/servizio-dati-slf/",
        "national",
        "Dataset dell'Istituto federale di ricerca per la foresta, la neve e il paesaggio (WSL) svizzero, rete partner di MeteoSwiss. Stazioni automatiche IMIS. Rilevazioni sub-orarie (UTC). Aggregazione di medie semiorarie.",
        "https://doi.org/10.16904/envidat.406"
    )
}

load_meta <- function(...) {
    vroom(fs::path(path.ds, "MeteoSwiss", "WSL", "imis", "stations.csv"),
        col_types = cols(
            network = col_character(),
            station_code = col_character(),
            label = col_character(),
            active = col_logical(),
            lon = col_double(),
            lat = col_double(),
            elevation = col_integer()
        ),
        locale = locale(encoding = "utf-8")
    ) |>
        as_tibble() |>
        mutate(
            dataset = "WSL",
            sensor_id = NA_character_,
            station_id = NA_character_,
            series_id = station_code,
            user_code = station_code,
            name = label,
            elevation = as.numeric(elevation),
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            town = NA_character_,
            district = NA_character_,
            province = NA_character_,
            country = "Switzerland",
            kind = "automatiche",
            .keep = "unused"
        )
}

load_data <- function() {
    data_dir <- fs::path(path.ds, "MeteoSwiss", "WSL", "imis", "data", "by_station")
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
