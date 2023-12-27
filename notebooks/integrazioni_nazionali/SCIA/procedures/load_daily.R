library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/load/read/SCIA.R")

dataset_spec <- function() {
    list(
        "SCIA",
        "http://193.206.192.214/servertsdailyutm/serietemporalidaily400.php",
        "national"
    )
}

load_daily_data.scia <- function(first_date, last_date) {
    tmin <- open_dataset(path.datafile("SCIA", "T_MIN")) |>
        rename(value = `Temperatura minima `) |>
        mutate(variable = "T_MIN")
    tmax <- open_dataset(path.datafile("SCIA", "T_MAX")) |>
        rename(value = `Temperatura massima `) |>
        mutate(variable = "T_MAX")

    stats <- read.SCIA.metadata("T_MAX")

    data <- concat_tables(tmin |> compute(), tmax |> compute(), unify_schemas = FALSE) |>
        filter(first_date <= date & date <= last_date) |>
        filter(!is.na(value)) |>
        mutate(station_id = as.character(internal_id), merged = FALSE, dataset = "SCIA") |>
        select(-internal_id) |>
        compute()

    meta <- read.SCIA.metadata("T_MAX") |>
        filter(lat > 42) |>
        select(!c(valid_days, last_year, first_year)) |>
        rename(id = identifier, network = rete, name = anagrafica) |>
        mutate(dataset = "SCIA", id = as.character(id), state = as.character(state), province = as.character(province), network = as.character(network)) |>
        as_arrow_table()

    data <- data |>
        semi_join(meta, join_by(dataset, station_id == id)) |>
        compute()
    meta <- meta |>
        semi_join(data, join_by(dataset, id == station_id)) |>
        compute()

    meta |>
        collect() |>
        group_by(id) |>
        tally() |>
        verify(n == 1L)

    n_dupli <- data |>
        group_by(dataset, station_id, variable, date) |>
        tally() |>
        filter(n > 1L) |>
        compute() |>
        nrow()

    if (n_dupli > 0) {
        stop("Duplicate measures in SCIA data")
    }

    # data <- data |>
    #     left_join(
    #         meta |>
    #             select(original_id, station_id) |>
    #             as_arrow_table(schema = schema(original_id = utf8(), station_id = utf8())),
    #         by = "original_id"
    #     ) |>
    #     mutate(merged = FALSE)

    list("meta" = meta, "data" = data)
}
