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
        "Dataset SIRMIP di ARPA Marche"
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
    meta <- vroom::vroom(file.path(arpam_path, "metadata.csv"), trim_ws = TRUE, delim = ",", col_types = "iccdic", show_col_types = FALSE) |>
        as_tibble()
    meta |>
        rename(
            lon = Longitudine,
            lat = Latitudine,
            elevation = `Quota [m]`,
            id = `Codice sensore`,
            name = `Nome stazione`,
        ) |>
        mutate(
            across(c(lon, lat), ~ str_replace(., "Ḟ", "°")),
            across(c(lon, lat), from_degrees),
            dataset = "ARPAM",
            network = "SIRMIP",
            state = "Marche",
        ) |>
        as_arrow_table()
}

load_data <- function(first_date, last_date) {
    arpam_path <- file.path(path.ds, "ARPA", "MARCHE")
    data <- open_dataset(file.path(arpam_path, "dataset"))
    data |>
        rename(station_id = codice_sensore, T_MIN = tmin, T_MAX = tmax) |>
        filter(quality > 90) |>
        select(!c(quality, num_valori, codice_stazione)) |>
        mutate(dataset = "ARPAM") |>
        to_duckdb() |>
        pivot_longer(c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        filter(first_date <= date & date <= last_date) |>
        to_arrow() |>
        as_arrow_table2(data_schema)
}

load_daily_data.arpam <- function(first_date, last_date) {
    data <- load_data(first_date, last_date)
    meta <- load_meta() |>
        semi_join(data, join_by(dataset, id == station_id)) |>
        compute()
    list("meta" = meta, "data" = data)
}
