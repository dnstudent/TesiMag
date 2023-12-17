source("src/load/read/BRUN.R")
source("src/database/tools.R")

load_work_metadata <- function() {
    tmin <- read.BRUN.metadata("T_MIN", "qc_era5") |>
        mutate(identifier = as.character(identifier), original_id = str_replace(identifier, regex("^(TMND_)|(TN_)"), ""))
    tmax <- read.BRUN.metadata("T_MAX", "qc_era5") |>
        mutate(identifier = as.character(identifier), original_id = str_replace(identifier, regex("^(TMXD_)|(TX_)"), ""))

    bind_rows(
        tmin,
        tmax
    ) |>
        filter(lat > 42) |>
        mutate(across(c(region_, country, province), as.character), dataset_id = "ISAC", network = "ISAC") |>
        rename(station_name = anagrafica, true_original_id = identifier) |>
        name_stations()
}

load_data <- function(meta) {
    tmin <- read.BRUN.series("T_MIN", "qc_era5") |>
        mutate(identifier = as.character(identifier)) |>
        semi_join(meta, join_by(identifier == true_original_id)) |>
        rename(value = T_MIN)

    tmax <- read.BRUN.series("T_MAX", "qc_era5") |>
        mutate(identifier = as.character(identifier)) |>
        semi_join(meta, join_by(identifier == true_original_id)) |>
        rename(value = T_MAX)

    bind_rows(
        T_MIN = tmin,
        T_MAX = tmax,
        .id = "variable"
    ) |>
        left_join(select(meta, true_original_id, station_id), join_by(identifier == true_original_id)) |>
        mutate(merged = FALSE) |>
        select(-identifier)
}

load_daily_data.isac <- function() {
    work_meta <- load_work_metadata()
    work_data <- load_data(work_meta)

    list("meta" = work_meta |> distinct(station_id, .keep_all = TRUE) |> as_arrow_table(), "data" = work_data |> as_arrow_table2(data_schema))
}
