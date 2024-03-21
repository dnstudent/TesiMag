setwd(fs::path_abs("~/Local_Workspace/TesiMag"))
library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tsibble, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")

dataset_spec <- function() {
    list(
        "https://www.osmer.fvg.it/archivio.php?ln=&p=dati",
        "regional",
        "Dataset di ARPA Friuli. Aggregazioni a passo giornaliero."
    )
}

path.base <- file.path(path.ds, "ARPA", "FVG")

load_osmer_meta <- function() {
    path <- file.path(path.base, "osmer_metas.json")
    jsonlite::fromJSON(path, flatten = TRUE)$fvg |>
        as_tibble() |>
        select(!c(DT_RowId, `0`, `1`, `2`, `3`, `4`, `5`, `6`, `8`, `9`)) |>
        rename_with(~ str_remove(., "properties."), .cols = starts_with("properties.")) |>
        rename(gestore = `7`, name = nome, user_code = sigla, kind = tecnologia, lat = latitudine, lon = longitudine, elevation = `altezza (m. s.l.m.)`, series_first = `data inizio`) |>
        mutate(lat = as.numeric(lat), lon = as.numeric(lon), elevation = as.numeric(elevation), series_first = ymd(series_first))
}

load_metadata <- function() {
    path.stats <- file.path(path.base, "station_info.csv")
    vroom::vroom(path.stats,
        col_types = vroom::cols(
            station_code = "c",
            station_name = "c",
            station_kind = "c",
            lat = "d",
            lon = "d",
            elevation = "d"
        )
    ) |>
        as_tibble() |>
        rename(station_id = station_code, name = station_name) |>
        mutate(
            dataset = "ARPAFVG",
            # state = "Friuli-Venezia Giulia",
            network = "ARPAFVG",
            kind = "unknown",
            user_code = station_id,
            town = NA_character_,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            sensor_id = NA_character_,
            series_id = station_id,
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_)
        )
}

load_data <- function() {
    path.data <- file.path(path.base, "dataset")
    # Ho valutato se pescare anche la T media per fare delle correzioni, ma meglio evitare mischiotti
    open_dataset(path.data) |>
        rename(station_id = stazione, T_MIN = `Temp. min gradi C`, T_MAX = `Temp. max gradi C`) |>
        select(T_MIN, T_MAX, station_id, date) |>
        mutate(station_id = cast(station_id, utf8())) |>
        to_duckdb() |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        filter(!is.na(value)) |>
        mutate(dataset = "ARPAFVG") |>
        to_arrow() |>
        compute()
}

load_daily_data.arpafvg <- function() {
    meta <- load_metadata()
    data <- load_data()

    if (data |> group_by(station_id, variable, date) |> tally() |> filter(n > 1L) |> compute() |> nrow() > 0) {
        stop("Duplicated values")
    }

    list("meta" = meta |> as_arrow_table(), "data" = data)
}
