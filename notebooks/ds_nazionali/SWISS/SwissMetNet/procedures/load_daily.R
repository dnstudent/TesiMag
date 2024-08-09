library(vroom, warn.conflicts = F)
library(arrow, warn.conflicts = F)
library(lubridate, warn.conflicts = F)

source("src/paths/paths.R")

dataset_spec <- function() {
    list(
        "https://opendata.swiss/it/dataset/automatische-wetterstationen-aktuelle-messwerte",
        "national",
        "Dataset delle stazioni automatiche svizzere. Frequenza di campionamento 10 min. Aggregazioni manuali. Recuperato da https://api.existenz.ch/"
    )
}

load_meta <- function() {
    vroom(fs::path(path.ds, "MeteoSwiss", "metadata.csv"),
        delim = ",",
        col_types = cols(
            `Altitudine stazione m slm` = col_integer(),
            CoordinateE = col_integer(),
            CoordinateN = col_integer(),
            Latitudine = col_double(),
            Longitudine = col_double(),
            `Dati dal` = col_date(),
            `Altitudine del barometro m da terra` = col_integer(),
            .default = col_character()
        ),
    ) |>
        as_tibble() |>
        rename(
            lon = Longitudine,
            lat = Latitudine,
            elevation = `Altitudine stazione m slm`,
            user_code = `WIGOS-ID`,
            series_id = `Abbr.`,
            name = Stazione,
            district = Cantone
        ) |>
        mutate(
            network = str_split(Proprietario, ","),
            dataset = "SwissMetNet",
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
            province = NA_character_,
            country = "Switzerland",
            kind = "unknown",
        ) |>
        rowwise() |>
        mutate(network = first(network)) |>
        ungroup()
}


load_data <- function() {
    data_dir <- fs::path(path.ds, "SwissMetNet", "fragments")

    open_csv_dataset(data_dir, col_names = TRUE, col_types = schema(
        result = utf8(),
        table = int8(),
        `_start` = arrow::timestamp(unit = "us"),
        `_stop` = arrow::timestamp(unit = "us"),
        `_time` = arrow::timestamp(unit = "us"),
        `_value` = float64(),
        `_field` = utf8(),
        `_measurement` = utf8(),
        loc = utf8()
    )) |>
        select(variable = result, date = `_time`, value = `_value`, series_id = loc) |>
        # Stazioni inesistenti in anagrafica. Hanno comunque pochi dati
        filter(!(series_id %in% c("MRP", "MSK"))) |>
        to_duckdb(conns$data) |>
        mutate(date = as.Date(date)) |>
        filter(!is.na(value)) |>
        tidyr::pivot_wider(id_cols = c("series_id", "date"), values_from = "value", names_from = "variable") |>
        mutate(count = as.integer(count)) |>
        # I dati sono forniti ogni 10 minuti, dovrebbero essere 144/giorno. Prendo i giorni con almeno il 90% di dati (ne restan fuori pochi)
        filter(count / 144 >= 0.9) |>
        select(-count) |>
        rename(T_MIN = tmin, T_MAX = tmax) |>
        tidyr::pivot_longer(cols = c("T_MIN", "T_MAX"), values_to = "value", names_to = "variable") |>
        to_arrow() |>
        mutate(dataset = "SwissMetNet") |>
        compute()
}

load_daily_data.swissmetnet <- function() {
    list(meta = load_meta(), data = load_data())
}
