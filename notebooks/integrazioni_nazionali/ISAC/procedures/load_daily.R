source("src/load/read/BRUN.R")
source("src/database/tools.R")
source("src/database/definitions.R")

dataset_spec <- function() {
    list(
        "locale",
        "national",
        "Dataset DPC+? fornito da Michele Brunetti. Utilizzo la versione qc con era5. Probabilmente per varie regioni registra gli estremi delle medie orarie, invece che gli estremi giornalieri."
    )
}

load_work_metadata <- function() {
    tmin <- read.BRUN.metadata("T_MIN", "qc_era5") |>
        mutate(identifier = as.character(identifier), id = str_replace(identifier, regex("^(TMND_)|(TN_)"), ""))
    tmax <- read.BRUN.metadata("T_MAX", "qc_era5") |>
        mutate(identifier = as.character(identifier), id = str_replace(identifier, regex("^(TMXD_)|(TX_)"), ""))

    bind_rows(
        tmin,
        tmax
    ) |>
        filter(lat > 42) |>
        mutate(across(c(region_, country, province), as.character), dataset = "ISAC", network = "ISAC") |>
        rename(name = anagrafica, actual_original_id = identifier)
}

load_data <- function(meta, first_date, last_date) {
    tmin <- read.BRUN.series("T_MIN", "qc_era5") |>
        rename(value = T_MIN)

    tmax <- read.BRUN.series("T_MAX", "qc_era5") |>
        rename(value = T_MAX)

    bind_rows(
        T_MIN = tmin,
        T_MAX = tmax,
        .id = "variable"
    ) |>
        drop_na(value) |>
        as_arrow_table() |>
        #     write_dataset("db.old/tmp/ISAC")
        # open_dataset("db.old/tmp/ISAC") |>
        rename(actual_original_id = identifier) |>
        filter(first_date <= date & date <= last_date) |>
        mutate(actual_original_id = cast(actual_original_id, utf8()), dataset = "ISAC") |>
        inner_join(
            meta |> select(actual_original_id, id) |> as_arrow_table(),
            by = "actual_original_id",
            relationship = "many-to-one"
        ) |>
        rename(station_id = id) |>
        as_arrow_table()
}

load_daily_data.isac <- function(first_date, last_date) {
    meta <- load_work_metadata()
    data <- load_data(meta, first_date, last_date)

    meta <- meta |>
        distinct(dataset, id, .keep_all = TRUE) |>
        as_arrow_table() |>
        semi_join(data, join_by(dataset, id == station_id)) |>
        compute()

    list("meta" = meta, "data" = data)
}
