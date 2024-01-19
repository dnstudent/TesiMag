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
    read_csv_arrow(file.path(path.ds, "ARPA", "LIGURIA", "metadata.csv"), as_data_frame = FALSE) |>
        mutate(network = ds_id, original_dataset = ds_id, state = "Liguria", across(c(anagrafica, identifier, province, BACINO), ~ cast(., utf8())), kind = "unknown") |>
        rename(name = anagrafica, original_id = identifier) |>
        compute()
}

load_data <- function() {
    read_parquet(file.path(path.ds, "ARPA", "LIGURIA", "dataset.parquet"), as_data_frame = FALSE) |>
        mutate(dataset = "ARPAL", across(c(variable, identifier), ~ cast(., utf8()))) |>
        rename(station_id = identifier) |>
        compute()
}

load_daily_data.arpal <- function() {
    meta <- load_meta()
    data <- load_data()

    meta <- semi_join(meta, data, join_by(original_id == station_id)) |> compute()

    list("meta" = meta, "data" = data)
}
