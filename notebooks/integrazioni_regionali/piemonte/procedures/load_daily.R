library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")

dataset_spec <- function() {
    list(
        "https://utility.arpa.piemonte.it/meteoidro/",
        "regional",
        "Dataset di ARPA Piemonte. Dati forniti con aggregazione giornaliera. Accesso tramite API."
    )
}

load_meta <- function() {
    col_names <- c(
        "data_inizio_sensore",
        "data_fine_sensore",
        "quota_da_pc",
        "altezza_supporto",
        "note_sensore",
        "codice_istat_comune",
        "codice_stazione",
        "denominazione",
        "indirizzo_localita",
        "nazione",
        "longitudine_e_wgs84_d",
        "latitudine_n_wgs84_d",
        "quota_stazione",
        "esposizione",
        "note",
        "tipo_staz",
        "data_inizio",
        "data_fine",
        "sigla_prov",
        "comune",
        "id_punto_misura"
    )

    meta_path <- file.path(path.ds, "ARPA", "PIEMONTE", "metadata.csv")
    read_csv_arrow(meta_path,
        col_select = all_of(col_names),
        timestamp_parsers = "%Y-%m-%d",
        as_data_frame = TRUE
    ) |>
        arrange(data_inizio_sensore) |>
        group_by(id_punto_misura) |>
        slice_tail() |> # There are duplicate ids; geolocations are identical
        ungroup() |>
        rename(id = id_punto_misura, elevation = quota_stazione, lon = longitudine_e_wgs84_d, lat = latitudine_n_wgs84_d, name = denominazione) |>
        mutate(dataset = "ARPAPiemonte", network = "ARPAPiemonte", state = "Piemonte") |>
        assertr::assert(assertr::is_uniq, id) |>
        as_arrow_table()
}

load_work_data <- function(first_date, last_date) {
    ds_path <- file.path(path.ds, "ARPA", "PIEMONTE", "site-data")
    open_dataset(ds_path) |>
        filter(first_date <= data & data <= last_date) |>
        select(data, tmax, tmin, id_punto) |>
        mutate(id_punto = cast(id_punto, utf8())) |>
        rename(station_id = id_punto, T_MIN = tmin, T_MAX = tmax, date = data)
}

load_daily_data.arpapiemonte <- function(first_date, last_date) {
    meta <- load_meta()
    data <- load_work_data(first_date, last_date) |>
        to_duckdb() |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        mutate(dataset = "ARPAPiemonte") |>
        filter(!is.na(value)) |>
        to_arrow() |>
        compute()

    meta <- semi_join(meta, data, join_by(dataset, id == station_id)) |> compute()

    list("meta" = meta, "data" = data)
}
