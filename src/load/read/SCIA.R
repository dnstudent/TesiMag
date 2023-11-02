library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(forcats, warn.conflicts = FALSE)

source("src/paths/paths.R")

read.SCIA.series <- function(tvar) {
    read_parquet(path.datafile("SCIA", tvar), as_data_frame = TRUE) |>
        rename_with(\(x) tvar, starts_with("Temperatura")) |>
        rename(identifier = internal_id)
}

read.SCIA.series.single <- function(tvar, id) {
    load.SCIA.series_(tvar) |> filter(identifier == as.integer(id))
}

read.SCIA.metadata <- function(tvar) {
    stations <- read_parquet(path.metadata("SCIA", tvar)) |>
        select(-wfs_name, -wfs_net_name, -chunk) |>
        mutate(state = fct_na_level_to_value(state, "None") |> as.character() |> as.factor() |> fct_recode("Friuli-Venezia Giulia" = "Friuli Venezia Giulia"), province = fct_na_level_to_value(province, "None") |> as.character() |> as.factor())
    corrections <- read_parquet(file.path(path.root("SCIA", tvar), SCIA.stations_corrections.relative(tvar))) |> select(-score)
    left_join(stations, corrections, by = "internal_id") |> rename(identifier = internal_id)
}
