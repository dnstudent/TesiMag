source("src/load/read/BRUN.R")
source("src/database/tools.R")

dataset_spec <- function() {
    list(
        "locale",
        "national",
        "Dataset DPC+ISAC fornito da Michele Brunetti. Utilizzo la versione qc con era5. Probabilmente per varie regioni registra gli estremi delle medie orarie, invece che gli estremi giornalieri."
    )
}

load_work_metadata <- function(variant) {
    tmin <- read.BRUN.metadata("T_MIN", variant) |>
        mutate(identifier = as.character(identifier), series_id = str_replace(identifier, regex("^(TMND_)|(TN_)"), ""))
    tmax <- read.BRUN.metadata("T_MAX", variant) |>
        mutate(identifier = as.character(identifier), series_id = str_replace(identifier, regex("^(TMXD_)|(TX_)"), ""))

    bind_rows(
        tmin,
        tmax
    ) |>
        # filter(lat > 42) |>
        mutate(across(c(region_, country, province), as.character), dataset = "ISAC", network = if_else(!is.na(internal_id), "DPC", "ISAC"), kind = "unknown") |>
        rename(name = anagrafica, actual_original_id = identifier)
}

load_data <- function(meta, variant) {
    tmin <- read.BRUN.series("T_MIN", variant) |>
        rename(value = T_MIN)

    tmax <- read.BRUN.series("T_MAX", variant) |>
        rename(value = T_MAX)

    bind_rows(
        T_MIN = tmin,
        T_MAX = tmax,
        .id = "variable"
    ) |>
        drop_na(value) |>
        as_arrow_table() |>
        rename(actual_original_id = identifier) |>
        mutate(actual_original_id = cast(actual_original_id, utf8()), dataset = "ISAC") |>
        inner_join(
            meta |> select(actual_original_id, series_id) |> as_arrow_table(),
            by = "actual_original_id",
            relationship = "many-to-one"
        ) |>
        select(-actual_original_id) |>
        as_arrow_table()
}

load_daily_data.isac <- function(variant = "raw") {
    meta <- load_work_metadata(variant)
    data <- load_data(meta, variant)

    meta <- meta |>
        distinct(dataset, series_id, .keep_all = TRUE) |>
        mutate(
            station_id = NA_character_,
            sensor_id = NA_character_,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            town = NA_character_,
        ) |>
        as_arrow_table()

    list("meta" = meta, "data" = data)
}
