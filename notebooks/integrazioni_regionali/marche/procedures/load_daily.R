library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/definitions.R")

dataset_spec <- function() {
    list(
        "http://app.protezionecivile.marche.it/sol/indexjs.sol?lang=it",
        "regional",
        "Dataset SIRMIP di ARPA Marche. Dati a passo giornaliero non validati."
    )
}

from_degrees <- function(value_str) {
    pieces <- value_str |>
        str_remove("'") |>
        str_split_fixed("°", 2L)

    as.numeric(pieces[, 1L]) + as.numeric(pieces[, 2L]) / 60
}

load_meta <- function() {
    arpam_path <- file.path(path.ds, "ARPA", "MARCHE")
    vroom::vroom(file.path(arpam_path, "metadata.csv"), trim_ws = TRUE, delim = ",", col_types = "iccdicc", show_col_types = FALSE) |>
        as_tibble() |>
        rename(series_id = original_id, station_id = ` Codice stazione`, network = kind) |>
        mutate(
            across(c(lon, lat), ~ str_replace(., "Ḟ", "°")),
            across(c(lon, lat), from_degrees),
            dataset = "ARPAM",
            state = "Marche",
            user_code = str_c(kind, "-", series_id),
            kind = case_match(kind, "RT" ~ "automatica", "RM" ~ "meccanica"),
            town = NA_character_,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
        ) |>
        as_arrow_table()
}

load_data <- function() {
    arpam_path <- file.path(path.ds, "ARPA", "MARCHE")
    data <- open_dataset(file.path(arpam_path, "dataset"))
    data |>
        rename(station_id = codice_sensore, T_MIN = tmin, T_MAX = tmax) |>
        filter(quality > 90) |>
        select(!c(quality, num_valori, codice_stazione)) |>
        to_duckdb() |>
        pivot_longer(c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        mutate(dataset = "ARPAM") |>
        as_arrow_table()
}

load_daily_data.arpam <- function() {
    data <- load_data()
    meta <- load_meta()
    list("meta" = meta, "data" = data)
}
