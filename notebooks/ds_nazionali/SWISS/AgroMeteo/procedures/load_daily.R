library(vroom, warn.conflicts = F)
library(arrow, warn.conflicts = F)
library(lubridate, warn.conflicts = F)

source("src/paths/paths.R")

dataset_spec <- function() {
    list(
        "https://www.agrometeo.ch/it/meteorologia",
        "national",
        "Dataset AgroMeteo svizzero"
    )
}

load_meta <- function() {
    # global_meta <- vroom(fs::path(path.ds, "MeteoSwiss", "metadata.csv"),
    #     delim = ",",
    #     col_types = cols(
    #         `Altitudine stazione m slm` = col_integer(),
    #         CoordinateE = col_integer(),
    #         CoordinateN = col_integer(),
    #         Latitudine = col_double(),
    #         Longitudine = col_double(),
    #         `Dati dal` = col_date(),
    #         .default = col_character(),
    #         `Altitudine del barometro m da terra` = col_integer()
    #     ),
    # ) |>
    #     as_tibble() |>
    #     rename(
    #         lon = Longitudine,
    #         lat = Latitudine,
    #         elevation = `Altitudine stazione m slm`,
    #         user_code = `WIGOS-ID`,
    #         series_id = `Abbr.`,
    #         name = Stazione,
    #         district = Cantone
    #     ) |>
    #     mutate(
    #         network = str_split(Proprietario, ","),
    #         dataset = "AgroMeteo",
    #         sensor_id = NA_character_,
    #         station_id = NA_character_,
    #         elevation = as.numeric(elevation),
    #         sensor_first = as.Date(NA_integer_),
    #         sensor_last = as.Date(NA_integer_),
    #         station_first = as.Date(NA_integer_),
    #         station_last = as.Date(NA_integer_),
    #         series_first = as.Date(NA_integer_),
    #         series_last = as.Date(NA_integer_),
    #         town = NA_character_,
    #         province = NA_character_,
    #         country = "Switzerland",
    #         kind = "unknown",
    #     )

    vroom(fs::path(path.ds, "MeteoSwiss", "AgroMeteo", "stations.csv"),
        col_types = cols(
            id = col_character(),
            long_dec = col_double(),
            lat_dec = col_double(),
            lat_ch = col_integer(),
            long_ch = col_integer(),
            altitude = col_double(),
            interval = col_integer(),
            into_service_at = col_date(format = "%Y-%m-%d"),
            type_id = col_integer(),
            owner_id = col_integer(),
            active = col_logical(),
            replacement_id = col_integer(),
            .default = col_character()
        )
    ) |>
        as_tibble() |>
        rename(lon = long_dec, lat = lat_dec, series_id = id, elevation = altitude) |>
        mutate(
            network = "AgroMeteo",
            dataset = "AgroMeteo",
            user_code = NA_character_,
            sensor_id = NA_character_,
            station_id = NA_character_,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            town = NA_character_,
            province = NA_character_,
            district = NA_character_,
            country = "Switzerland",
            kind = "unknown",
        )
}


load_data <- function() {
    data_dir <- fs::path(path.ds, "MeteoSwiss", "AgroMeteo", "dataset")

    rename_var <- tibble(aggregation = c("max", "min"), variable = c("T_MAX", "T_MIN")) |> as_arrow_table()
    open_dataset(data_dir, format = "parquet") |>
        select(-variable) |>
        filter(!is.na(value)) |>
        mutate(series_id = as.character(station_id), dataset = "AgroMeteo", aggregation = as.character(aggregation)) |>
        left_join(rename_var, by = "aggregation") |>
        select(-aggregation, -station_id) |>
        compute()
}

load_daily_data.agrometeo <- function() {
    list(meta = load_meta(), data = load_data())
}
