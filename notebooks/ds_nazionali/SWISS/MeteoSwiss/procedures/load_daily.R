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
        rename(
            lon = Longitude,
            lat = Latitude,
            elevation = `Station height m. a. sea level`,
            user_code = `station/location`,
            series_id = `WIGOS-ID`,
            name = Station
        ) |>
        mutate(
            network = "NBNC",
            dataset = "MeteoSwiss",
            sensor_id = NA_character_,
            station_id = NA_character_,
            elevation = as.numeric(elevation),
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            town = NA_character_,
            state = NA_character_,
            province = NA_character_,
            country = "Switzerland",
            kind = "unknown",
        )
}


load_data <- function() {
    data_dir <- fs::path(path.ds, "MeteoSwiss", "NBNC-daily", "fragments")
    open_delim_dataset(data_dir,
        schema = schema(
            `station/location` = utf8(),
            date = int32(),
            gre000d0 = utf8(),
            hto000d0 = utf8(),
            nto000d0 = utf8(),
            prestad0 = utf8(),
            rre150d0 = utf8(),
            sre000d0 = utf8(),
            tre200d0 = utf8(),
            tre200dn = float64(),
            tre200dx = float64(),
            ure200d0 = utf8()
        ),
        na = c("-"),
        delim = ";",
        skip = 1L
    ) |>
        select(user_code = `station/location`, date, T_MIN = tre200dn, T_MAX = tre200dx) |>
        mutate(date = lubridate::ymd(date)) |>
        to_duckdb() |>
        tidyr::pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        filter(!is.na(value)) |>
        mutate(dataset = "MeteoSwiss") |>
        compute()
}

load_daily_data.meteoswiss <- function() {
    meta <- load_meta()
    data <- load_data() |>
        left_join(meta |> select(user_code, series_id) |> as_arrow_table(), by = "user_code") |>
        select(-user_code) |>
        compute()

    list(meta = meta, data = data)
}
