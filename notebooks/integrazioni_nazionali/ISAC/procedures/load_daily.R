source("src/load/read/BRUN.R")
source("src/database/tools.R")
source("src/database/definitions.R")

dataset_spec <- function() {
    list(
        "locale",
        "national",
        "Dataset DPC+ISAC fornito da Michele Brunetti. Utilizzo la versione qc con era5. Probabilmente per varie regioni registra gli estremi delle medie orarie, invece che gli estremi giornalieri."
    )
}

load_work_metadata <- function() {
    tmin <- read.BRUN.metadata("T_MIN", "raw") |>
        mutate(identifier = as.character(identifier), original_id = str_replace(identifier, regex("^(TMND_)|(TN_)"), ""))
    tmax <- read.BRUN.metadata("T_MAX", "raw") |>
        mutate(identifier = as.character(identifier), original_id = str_replace(identifier, regex("^(TMXD_)|(TX_)"), ""))

    bind_rows(
        tmin,
        tmax
    ) |>
        # filter(lat > 42) |>
        mutate(across(c(region_, country, province), as.character), original_dataset = "ISAC", network = if_else(!is.na(internal_id), "DPC", "ISAC"), kind = "unknown") |>
        rename(name = anagrafica, actual_original_id = identifier)
}

load_data <- function(meta) {
    tmin <- read.BRUN.series("T_MIN", "raw") |>
        rename(value = T_MIN)

    tmax <- read.BRUN.series("T_MAX", "raw") |>
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
        mutate(actual_original_id = cast(actual_original_id, utf8()), dataset = "ISAC") |>
        inner_join(
            meta |> select(actual_original_id, original_id) |> as_arrow_table(),
            by = "actual_original_id",
            relationship = "many-to-one"
        ) |>
        rename(station_id = original_id) |>
        as_arrow_table()
}

load_daily_data.isac <- function() {
    meta <- load_work_metadata()
    data <- load_data(meta)

    meta <- meta |>
        distinct(original_dataset, original_id, .keep_all = TRUE) |>
        as_arrow_table()

    list("meta" = meta, "data" = data)
}
