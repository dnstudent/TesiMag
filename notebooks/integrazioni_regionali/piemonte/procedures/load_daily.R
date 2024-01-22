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

load_meta_meteo <- function() {
    read_csv_arrow(file.path(path.ds, "ARPA", "PIEMONTE", "meteo", "meta.csv"),
        timestamp_parsers = "%Y-%m-%d",
        as_data_frame = FALSE
    ) |>
        rename(fk_id_punto_misura = fk_id_punto_misura_meteo, original_id = codice_punto_misura, elevation = quota_stazione, lon = longitudine_e_wgs84_d, lat = latitudine_n_wgs84_d, name = denominazione, kind = tipo_staz) |>
        mutate(network = "Meteo") |>
        compute()
}

load_meta_centenaria <- function() {
    read_csv_arrow(file.path(path.ds, "ARPA", "PIEMONTE", "centenaria", "meta.csv"),
        timestamp_parsers = "%Y-%m-%d",
        as_data_frame = FALSE
    ) |>
        rename(url_stazione = url, fk_id_punto_misura = fk_id_punto_misura_centenaria, original_id = codice_punto_misura, elevation = quota_stazione, lon = longitudine_e_wgs84_d, lat = latitudine_n_wgs84_d, name = denominazione, kind = tipo_staz) |>
        mutate(network = "Centenaria", codice_stazione = cast(codice_stazione, utf8()), data_inizio_sensore = data_inizio) |>
        select(!c(the_geom)) |>
        compute()
}

load_meta <- function() {
    load_meta_meteo() |>
        concat_tables(load_meta_centenaria()) |>
        mutate(original_dataset = "ARPAPiemonte", state = "Piemonte", elevation = cast(elevation, float64())) |>
        collect() |>
        group_by(original_id) |>
        arrange(data_inizio_sensore) |>
        mutate(sensor_id = paste0(original_id, "_", row_number()), n_sensors = n()) |>
        ungroup() |>
        mutate(new_id = if_else(n_sensors == 1L, original_id, sensor_id)) |>
        mutate(data_fine_sensore = coalesce(data_fine_sensore, as.Date("2024-01-22"))) |>
        as_arrow_table() |>
        compute()
}

dataset_meteo <- function() {
    open_dataset(
        file.path(path.ds, "ARPA", "PIEMONTE", "meteo", "fragments"),
        format = "csv",
        skip = 1L,
        schema = schema(
            data = date32(),
            tmedia = float64(),
            tmax = float64(),
            tmin = float64(),
            tclasse = utf8(),
            ptot = utf8(),
            pclasse = utf8(),
            vmedia = utf8(),
            vraffica = utf8(),
            settore_prevalente = utf8(),
            tempo_permanenza = utf8(),
            durata_calma = utf8(),
            vclasse = utf8(),
            umedia = utf8(),
            umin = utf8(),
            umax = utf8(),
            uclasse = utf8(),
            rtot = utf8(),
            rclasse = utf8(),
            fk_id_punto_misura_meteo = utf8()
        )
    ) |>
        select(date = data, T_MAX = tmax, T_MIN = tmin, id_punto_misura = fk_id_punto_misura_meteo)
}

dataset_centenaria <- function() {
    open_dataset(
        file.path(path.ds, "ARPA", "PIEMONTE", "centenaria", "fragments"),
        format = "csv"
    ) |>
        select(date = data, T_MAX = omog_tmax, T_MIN = omog_tmin, id_punto_misura = fk_id_punto_misura_centenaria)
}

load_work_data <- function() {
    concat_tables(
        dataset_meteo() |> compute(),
        dataset_centenaria() |> compute(),
        unify_schemas = FALSE
    ) |>
        to_duckdb() |>
        mutate(station_id = str_sub(id_punto_misura, -15L, -2L), dataset = "ARPAPiemonte", .keep = "unused") |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        compute()
}

load_daily_data.arpapiemonte <- function() {
    meta <- load_meta()
    data <- load_work_data() |>
        left_join(load_meta() |> select(original_id, new_id, data_inizio_sensore, data_fine_sensore), by = c("station_id" = "original_id")) |>
        filter(between(date, data_inizio_sensore, data_fine_sensore)) |>
        select(dataset, station_id = new_id, date, variable, value) |>
        compute()
    list("meta" = meta, "data" = data)
}
