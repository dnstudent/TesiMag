library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")

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
        rename(original_id = id_punto_misura, elevation = quota_stazione, lon = longitudine_e_wgs84_d, lat = latitudine_n_wgs84_d, station_name = denominazione) |>
        mutate(dataset_id = "ARPAPiemonte", network = "ARPAPiemonte", state = "Piemonte") |>
        name_stations() |>
        assertr::assert(assertr::is_uniq, station_id) |>
        as_arrow_table()
}

load_work_data <- function() {
    ds_path <- file.path(path.ds, "ARPA", "PIEMONTE", "site-data")
    open_dataset(ds_path) |>
        select(data, tmax, tmin, id_punto) |>
        mutate(id_punto = cast(id_punto, utf8())) |>
        rename(original_id = id_punto, T_MIN = tmin, T_MAX = tmax, date = data)
}

load_daily_data.arpapiemonte <- function() {
    meta <- load_meta() |> as_arrow_table2(station_schema)
    data <- load_work_data() |>
        left_join(meta |> select(original_id, station_id), by = "original_id") |>
        select(-original_id) |>
        collect() |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        mutate(merged = FALSE) |>
        as_arrow_table2(data_schema)

    list("meta" = meta, "data" = data)
}
