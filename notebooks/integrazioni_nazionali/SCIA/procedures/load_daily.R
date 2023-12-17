library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/load/read/SCIA.R")

load_daily_data.scia <- function() {
    tmin <- open_dataset(path.datafile("SCIA", "T_MIN")) |>
        rename(value = `Temperatura minima `) |>
        mutate(variable = "T_MIN")
    tmax <- open_dataset(path.datafile("SCIA", "T_MAX")) |>
        rename(value = `Temperatura massima `) |>
        mutate(variable = "T_MAX")
    stats <- read.SCIA.metadata("T_MAX")

    data <- concat_tables(tmin |> compute(), tmax |> compute(), unify_schemas = FALSE) |>
        mutate(original_id = as.character(internal_id), merged = FALSE) |>
        select(-internal_id)

    meta <- read.SCIA.metadata("T_MAX") |>
        select(!c(valid_days, last_year, first_year)) |>
        rename(original_id = identifier, network = rete, station_name = anagrafica) |>
        mutate(dataset_id = "SCIA", original_id = as.character(original_id), state = as.character(state), province = as.character(province), network = as.character(network)) |>
        name_stations()

    meta |>
        group_by(station_id) |>
        tally() |>
        verify(n == 1L)
    meta |>
        group_by(original_id) |>
        tally() |>
        verify(n == 1L)

    data <- data |>
        left_join(
            meta |>
                select(original_id, station_id) |>
                as_arrow_table(schema = schema(original_id = utf8(), station_id = utf8())),
            by = "original_id"
        ) |>
        mutate(merged = FALSE)

    list("meta" = meta |> as_arrow_table(), "data" = data |> as_arrow_table2(data_schema))
}
