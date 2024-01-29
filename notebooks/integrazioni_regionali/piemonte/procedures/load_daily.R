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
        col_types = schema(
            url = utf8(),
            url_sensore = utf8(),
            id_parametro = utf8(),
            id_sensore = utf8(),
            data_inizio_sensore = date32(),
            data_fine_sensore = date32(),
            quota_da_pc = float(),
            altezza_supporto = float(),
            note_sensore = utf8(),
            fk_id_stazione_meteorologica = utf8(),
            codice_istat_comune = utf8(),
            codice_stazione = utf8(),
            denominazione = utf8(),
            indirizzo_localita = utf8(),
            nazione = utf8(),
            longitudine_e_wgs84_d = float64(),
            latitudine_n_wgs84_d = float64(),
            quota_stazione = float64(),
            esposizione = utf8(),
            note = utf8(),
            tipo_staz = utf8(),
            data_inizio = date32(),
            data_fine = date32(),
            sigla_prov = utf8(),
            comune = utf8(),
            fk_id_punto_misura_meteo = utf8(),
            id_sensore = utf8(),
            id_stazione_meteo = utf8(),
            id_punto_misura_meteo = utf8()
        ),
        as_data_frame = TRUE
    ) |>
        rename(
            sensor_id = id_sensore,
            sensor_first = data_inizio_sensore,
            sensor_last = data_fine_sensore,
            station_id = id_stazione_meteo,
            name = denominazione,
            town = comune,
            station_first = data_inizio,
            station_last = data_fine,
            province = sigla_prov,
            user_code = codice_stazione,
            kind = tipo_staz,
            lon = longitudine_e_wgs84_d,
            lat = latitudine_n_wgs84_d,
            elevation = quota_stazione,
            series_id = id_punto_misura_meteo,
            url_stazione = url,
            fk_id_punto_misura = fk_id_punto_misura_meteo,
            fk_id_stazione = fk_id_stazione_meteorologica,
        )
}

load_meta_centenaria <- function() {
    read_csv_arrow(file.path(path.ds, "ARPA", "PIEMONTE", "centenaria", "meta.csv"),
        col_types = schema(
            url = utf8(),
            codice_istat_comune = utf8(),
            codice_stazione = utf8(),
            denominazione = utf8(),
            indirizzo_localita = utf8(),
            nazione = utf8(),
            sigla_prov = utf8(),
            comune = utf8(),
            longitudine_e_wgs84_d = float64(),
            latitudine_n_wgs84_d = float64(),
            quota_stazione = float64(),
            esposizione = utf8(),
            tipo_staz = utf8(),
            data_inizio = date32(),
            data_fine = date32(),
            note = utf8(),
            the_geom = utf8(),
            fk_id_punto_misura_centenaria = utf8(),
            id_stazione_centenaria = utf8(),
            id_punto_misura_centenaria = utf8(),
        ),
        timestamp_parsers = "%Y-%m-%d",
        as_data_frame = TRUE
    ) |>
        rename(
            url_stazione = url,
            fk_id_punto_misura = fk_id_punto_misura_centenaria,
            series_id = id_punto_misura_centenaria,
            station_id = id_stazione_centenaria,
            lon = longitudine_e_wgs84_d,
            lat = latitudine_n_wgs84_d,
            elevation = quota_stazione,
            name = denominazione,
            town = comune,
            kind = tipo_staz,
            user_code = codice_stazione,
            province = sigla_prov,
            station_first = data_inizio,
            station_last = data_fine,
        ) |>
        mutate(series_first = station_first, series_last = station_last) |>
        select(!c(the_geom))
}

load_meta_nivo <- function(meta_meteo) {
    read_csv_arrow(file.path(path.ds, "ARPA", "PIEMONTE", "nivo", "meta.csv"),
        timestamp_parsers = "%Y-%m-%d",
        as_data_frame = TRUE,
        col_types = schema(
            url = utf8(),
            codice_istat_comune = utf8(),
            codice_stazione = utf8(),
            denominazione = utf8(),
            indirizzo_localita = utf8(),
            nazione = utf8(),
            longitudine_e_wgs84_d = float64(),
            latitudine_n_wgs84_d = float64(),
            quota_stazione = float64(),
            note = utf8(),
            tipo_staz = utf8(),
            settore_alpino = utf8(),
            campo_neve = utf8(),
            data_inizio = date32(),
            data_fine = date32(),
            sigla_prov = utf8(),
            comune = utf8(),
            fk_id_punto_misura_nivo = utf8(),
            id_stazione_nivo = utf8(),
            id_punto_misura_nivo = utf8(),
        ),
    ) |>
        rename(
            url_stazione = url,
            fk_id_punto_misura = fk_id_punto_misura_nivo,
            station_id = id_stazione_nivo,
            name = denominazione,
            town = comune,
            province = sigla_prov,
            station_first = data_inizio,
            station_last = data_fine,
            user_code = codice_stazione,
            kind = tipo_staz,
            lon = longitudine_e_wgs84_d,
            lat = latitudine_n_wgs84_d,
            elevation = quota_stazione,
            series_id = id_punto_misura_nivo,
        ) |>
        mutate(series_first = station_first, series_last = station_last) |>
        select(-campo_neve, -settore_alpino) |>
        anti_join(meta_meteo, by = c("series_id"))
}

load_meta <- function() {
    meteo_meta <- load_meta_meteo()
    bind_rows("meteorologica" = meteo_meta, "centenaria" = load_meta_centenaria(), "nivo" = load_meta_nivo(meteo_meta), .id = "network") |>
        mutate(dataset = "ARPAPiemonte")
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
        select(date = data, T_MAX = tmax, T_MIN = tmin, fk_id_punto_misura = fk_id_punto_misura_meteo)
}

dataset_centenaria <- function() {
    open_dataset(
        file.path(path.ds, "ARPA", "PIEMONTE", "centenaria", "fragments"),
        format = "csv"
    ) |>
        select(date = data, T_MAX = omog_tmax, T_MIN = omog_tmin, fk_id_punto_misura = fk_id_punto_misura_centenaria)
}

dataset_nivo <- function() {
    open_dataset(
        file.path(path.ds, "ARPA", "PIEMONTE", "nivo", "fragments"),
        format = "csv"
    ) |>
        select(date = data, T_MAX = tmax, T_MIN = tmin, fk_id_punto_misura = fk_id_punto_misura_nivo)
}

load_data <- function() {
    concat_tables(
        dataset_meteo() |> compute(),
        dataset_centenaria() |> compute(),
        dataset_nivo() |> compute(),
        unify_schemas = FALSE
    ) |>
        to_duckdb() |>
        mutate(series_id = str_sub(fk_id_punto_misura, -15L, -2L), dataset = "ARPAPiemonte", .keep = "unused") |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        compute()
}

load_daily_data.arpapiemonte <- function() {
    meta <- load_meta()
    data <- load_data()
    list("meta" = meta, "data" = data)
}
