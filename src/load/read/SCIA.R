library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(forcats, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)

source("src/paths/paths.R")

read.SCIA.series <- function(tvar) {
    read_parquet(path.datafile("SCIA", tvar), as_data_frame = FALSE) |>
        rename_with(\(x) tvar, starts_with("Temperatura")) |>
        rename(identifier = internal_id)
}

read.SCIA.series.single <- function(tvar, id) {
    series_path <- file.path("scia_split", tvar, paste0(as.integer(id), ".parquet"))
    read_parquet(series_path, as_data_frame = TRUE)
}

read.SCIA.series.bunch <- function(tvar, ids, many = FALSE) {
    ids <- as.integer(ids)
    series_paths <- file.path("scia_split", tvar, paste0(ids, ".parquet"))
    as_tibble(ids) |>
        rename(myid = 1) |>
        rowwise() |>
        reframe(data = read.SCIA.series.single(tvar, myid)) |>
        unnest(data)
}

read.SCIA.metadata <- function(tvar) {
    stations <- read_parquet(path.metadata("SCIA", tvar)) |>
        select(-wfs_name, -wfs_net_name, -chunk) |>
        mutate(state = fct_na_level_to_value(state, "None") |> as.character() |> as.factor() |> fct_recode("Friuli-Venezia Giulia" = "Friuli Venezia Giulia"), province = fct_na_level_to_value(province, "None") |> as.character() |> as.factor())
    corrections <- read_parquet(file.path(path.root("SCIA", tvar), SCIA.stations_corrections.relative(tvar))) |> select(-score)
    left_join(stations, corrections, by = "internal_id") |> rename(identifier = internal_id)
}
