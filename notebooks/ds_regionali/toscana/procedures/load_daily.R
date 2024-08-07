library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(vroom, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")

dataset_spec <- function() {
    list(
        "http://www.sir.toscana.it/consistenza-rete",
        "regional",
        "Dataset del SIR Toscana. Passo Giornaliero. Le stazioni selezionate comprendono quelle categorizzate come Automatiche, Automatiche SIR e Tradizionali."
    )
}

path.toscana <- file.path(path.ds, "ARPA", "TOSCANA")

load_meta <- function() {
    read_delim_arrow(
        file.path(path.toscana, "stazioni.csv"),
        # col_names = ,
        col_types = "cicccccciddddddc",
        delim = ",",
        read_options = csv_read_options(
            skip_rows = 1L,
            column_names = c("series_id", "StazioneExtra", "name", "town", "province_code", "Fiume", "Strumento", "Unita_Misura", "sensor_id", "lat", "lon", "GB E [m]", "GB N [m]", "elevation", "QuotaTerra", "network"),
            encoding = "utf-8"
        ),
        as_data_frame = TRUE
    ) |>
        filter(Strumento == "termometro") |>
        mutate(
            kind = case_match(network, "Tradizionali" ~ "meccanica", .default = "automatica"),
            dataset = "SIRToscana",
            network = str_c("SIRT - ", network),
            station_id = series_id,
            user_code = series_id,
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
    vroom(
        list.files(file.path(path.toscana, "fragments"), pattern = "*.csv", full.names = TRUE),
        skip = 19L,
        col_names = c("date", "T_MAX", "T_MIN"),
        col_types = cols(
            col_date(format = "%d/%m/%Y"),
            col_double(),
            col_double()
        ),
        delim = ";",
        na = c("", "NA", "-9999", "@"),
        id = "path"
    ) |>
        as_tibble() |>
        mutate(dataset = "SIRToscana", station_id = path |> basename() |> str_remove("\\.csv$"), .keep = "unused") |>
        pivot_longer(cols = c(T_MAX, T_MIN), names_to = "variable", values_to = "value") |>
        drop_na(value) |>
        as_arrow_table()
}

load_daily_data.toscana <- function() {
    meta <- load_meta()
    data <- load_data() |>
        semi_join(meta, by = "station_id") |>
        compute()
    meta <- meta |>
        semi_join(data, by = "station_id") |>
        compute()

    list("meta" = meta, "data" = data)
}
