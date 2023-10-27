library(arrow)
library(dplyr)
library(tsibble)
library(forcats)
library(sf)

source("paths.R")

load.SCIA._series <- function(tvar) {
    read_parquet(path.series("SCIA", tvar), as_data_frame = TRUE) |>
        rename_with(\(x) tvar, starts_with("Temperatura")) |>
        rename(identifier = internal_id)
}

load.SCIA.series.single <- function(var, id) {
    load.SCIA._series(var) |> filter(identifier == as.integer(id))
}

load.SCIA._metadata <- function(tvar) {
    stations <- read_parquet(path.stations("SCIA", tvar)) |>
        select(-wfs_name, -wfs_net_name, -chunk) |>
        mutate(state = fct_na_level_to_value(state, "None") |> as.character() |> as.factor() |> fct_recode("Friuli-Venezia Giulia" = "Friuli Venezia Giulia"), province = fct_na_level_to_value(province, "None") |> as.character() |> as.factor())
    corrections <- read_parquet(path.SCIA.stations.corrections(tvar)) |> select(-score)
    left_join(stations, corrections, by = "internal_id") |> rename(identifier = internal_id)
}
