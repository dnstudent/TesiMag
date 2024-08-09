library(vroom, warn.conflicts = F)
library(arrow, warn.conflicts = F)
library(lubridate, warn.conflicts = F)

source("src/paths/paths.R")

load_general_meta <- function() {
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
            province = NA_character_,
            country = "Switzerland",
            kind = "unknown",
        ) |>
        rowwise() |>
        mutate(network = first(network)) |>
        ungroup() |>
        arrange(series_id)
}
