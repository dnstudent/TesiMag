library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/paths/paths.R")

dataset_spec <- function() {
    list(
        "https://ambientepub.regione.liguria.it/SiraQualMeteo/script/PubAccessoDatiMeteo.asp",
        "regional",
        "Dataset di ARPA Liguria. Misure giornaliere aggregate su orari UTC (?)"
    )
}

load_meta <- function() {
    ds_id <- "ARPAL"
    read_feather(file.path(path.ds, "ARPA", "LIGURIA", "metadata.arrow"), as_data_frame = FALSE) |>
        mutate(network = ds_id, dataset = ds_id, state = "Liguria", across(c(anagrafica, identifier, province, BACINO), ~ cast(., utf8()))) |>
        rename(name = anagrafica, id = identifier) |>
        collect() |>
        as_arrow_table()
}

load_data <- function(first_date, last_date) {
    read_parquet(file.path(path.ds, "ARPA", "LIGURIA", "dataset.parquet"), as_data_frame = FALSE) |>
        filter(first_date <= date & date <= last_date) |>
        mutate(dataset = "ARPAL", across(c(variable, identifier), ~ cast(., utf8()))) |>
        rename(station_id = identifier) |>
        compute()
}

load_daily_data.arpal <- function(first_date, last_date) {
    meta <- load_meta()
    data <- load_data(first_date, last_date)

    meta <- semi_join(meta, data, join_by(dataset, id == station_id)) |> compute()

    list("meta" = meta, "data" = data)
}
