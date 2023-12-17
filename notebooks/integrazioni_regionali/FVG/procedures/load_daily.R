setwd(fs::path_abs("~/Local_Workspace/TesiMag"))
library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tsibble, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")

path.base <- file.path(path.ds, "ARPA", "FVG")

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
        rename(original_id = station_code) |>
        mutate(dataset_id = "ARPAFVG", state = "Friuli-Venezia Giulia", network = "ARPAFVG") |>
        mutate(station_id = as.character(row_number())) |>
        name_stations()
}

load_data <- function() {
    path.data <- file.path(path.base, "dataset")
    # Ho valutato se pescare anche la T media per fare delle correzioni, ma meglio evitare mischiotti
    open_dataset(path.data) |>
        rename(original_id = stazione, T_MIN = `Temp. min gradi C`, T_MAX = `Temp. max gradi C`) |>
        select(T_MIN, T_MAX, original_id, date) |>
        mutate(original_id = cast(original_id, utf8())) |>
        filter(!(is.na(T_MIN) & is.na(T_MAX))) |>
        collect() |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        mutate(merged = FALSE)
}

load_daily_data.arpafvg <- function() {
    meta <- load_metadata()
    data <- load_data() |>
        left_join(meta |> select(original_id, station_id), by = "original_id") |>
        select(-original_id)

    if (data |> duplicates(key = c(station_id, variable), index = date) |> nrow() > 0) {
        stop("Duplicated values")
    }

    list("meta" = meta |> as_arrow_table(), "data" = data |> as_arrow_table2(data_schema))
}
