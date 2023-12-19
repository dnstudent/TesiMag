library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/paths/paths.R")

load_meta <- function() {
    ds_id <- "ARPAL"
    read_feather(file.path(path.ds, "ARPA", "LIGURIA", "metadata.arrow"), as_data_frame = FALSE) |>
        mutate(network = ds_id, dataset_id = ds_id, state = "Liguria", across(c(anagrafica, identifier, province, BACINO), ~ cast(., utf8()))) |>
        rename(station_name = anagrafica, original_id = identifier) |>
        collect() |>
        name_stations() |>
        as_arrow_table()
}

load_data <- function() {
    data <- read_parquet(file.path(path.ds, "ARPA", "LIGURIA", "dataset.parquet"), as_data_frame = FALSE) |>
        mutate(merged = FALSE, across(c(variable, identifier), ~ cast(., utf8())))
}

load_daily_data.arpal <- function() {
    meta <- load_meta()
    data <- load_data() |>
        left_join(meta |> select(original_id, station_id), join_by(identifier == original_id)) |>
        as_arrow_table2(data_schema)

    list("meta" = meta, "data" = data)
}
