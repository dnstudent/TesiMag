library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(vroom, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/definitions.R")
source("src/database/tools.R")

dataset_spec <- function() {
    list(
        "http://www.sir.toscana.it/consistenza-rete",
        "regional",
        "Dataset del SIR Toscana. Passo Giornaliero. Le stazioni selezionate comprendono quelle categorizzate come Automatiche e Tradizionali."
    )
}

path.toscana <- file.path(path.ds, "ARPA", "TOSCANA")

load_meta <- function() {
    read_delim_arrow(
        file.path(path.toscana, "stazioni.csv"),
        # col_names = ,
        col_types = "ciccccccidddddd",
        delim = ";",
        read_options = csv_read_options(
            skip_rows = 1L,
            column_names = c("id", "StazioneExtra", "name", "Comune", "province", "Fiume", "Strumento", "Unita_Misura", "IDSensoreRete", "lat", "lon", "GB E [m]", "GB N [m]", "elevation", "QuotaTerra"),
            encoding = "iso-8859-1"
        ),
        as_data_frame = TRUE
    ) |>
        mutate(dataset = "SIRToscana", network = "SIRToscana", state = "Toscana") |>
        as_arrow_table()
}

load_data <- function(first_date, last_date) {
    vroom(
        list.files(file.path(path.toscana, "fragments"), pattern = "*.csv", full.names = TRUE),
        skip = 19L,
        col_names = c("date", "T_MAX", "T_MIN"),
        col_types = cols(
            date = col_date(format = "%d/%m/%Y"),
            T_MAX = col_double(),
            T_MIN = col_double()
        ),
        delim = ";",
        na = c("", "NA", "-9999", "@"),
        id = "path"
    ) |>
        as_tibble() |>
        filter(first_date <= date & date <= last_date) |>
        mutate(dataset = "SIRToscana", station_id = path |> basename() |> str_remove("\\.csv$"), .keep = "unused") |>
        pivot_longer(cols = c(T_MAX, T_MIN), names_to = "variable", values_to = "value") |>
        drop_na(value) |>
        as_arrow_table()
}

load_daily_data.toscana <- function(first_date, last_date) {
    data <- load_data(first_date, last_date)
    meta <- load_meta() |>
        semi_join(data, join_by(dataset, id == station_id)) |>
        compute()

    list("meta" = meta, "data" = data)
}
