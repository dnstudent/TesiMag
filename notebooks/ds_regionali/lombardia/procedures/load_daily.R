library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(duckdb, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/database/query/data.R")
source("src/paths/paths.R")
source("src/analysis/data/quality_check.R")

dataset_spec <- function() {
    list(
        "https://www.dati.lombardia.it/Ambiente/Dati-sensori-meteo-2020/erjn-istm/about_data",
        "regional",
        "Dataset di ARPA Lombardia. Dati suborari. Aggregazione effettuata manualmente. Dati misurati fino all''orario indicato, ora solare."
    )
}

path.lom <- file.path(path.ds, "ARPA", "LOMBARDIA")

load_work_metadata.arpalombardia <- function() {
    path.md <- file.path(path.lom, "Stazioni_Meteorologiche.tsv")
    read_tsv_arrow(path.md,
        schema = schema(
            IdSensore = int32(),
            Tipologia = utf8(),
            `Unità DiMisura` = utf8(),
            IdStazione = int32(),
            NomeStazione = utf8(),
            Quota = float64(),
            Provincia = utf8(),
            DataStart = timestamp(unit = "s"),
            DataStop = timestamp(unit = "s"),
            Storico = utf8(),
            UTM_Nord = int32(),
            UTM_Est = int32(),
            lng = float64(),
            lat = float64(),
            location = utf8(),
        ),
        skip = 1L,
        timestamp_parsers = "%d/%m/%Y",
        as_data_frame = FALSE,
    ) |>
        select(-location) |>
        filter(Tipologia == "Temperatura") |>
        mutate(across(starts_with("Data"), ~ cast(., date32())), network = "ARPALombardia", dataset = "ARPALombardia", kind = "unknown") |>
        rename(
            sensor_id = IdSensore,
            name = NomeStazione,
            province_code = Provincia,
            elevation = Quota,
            lon = lng,
            type = Tipologia,
            station_id = IdStazione,
            sensor_first = DataStart,
            sensor_last = DataStop
        ) |>
        mutate(
            station_first = sensor_first,
            station_last = sensor_last,
            series_first = sensor_first,
            series_last = sensor_last,
            town = NA_character_,
            user_code = str_pad(as.character(station_id), width = 5L, side = "left", pad = "0"),
        ) |>
        compute()
}

load_hexts <- function(dataconn, metadata) {
    metadata <- metadata |> to_duckdb(con = dataconn)
    # query_parquet(list.files(file.path(path.lom, "dataset"), full.names = TRUE), conn = conn) |>
    #     mutate(station_id = as.integer(station_id)) |>
    #     filter(!is.na(value), idOperatore == 1L) |>
    #     rename(sensor_id = station_id) |>
    #     semi_join(metadata, by = "sensor_id") |>
    #     mutate(time = time - sql("INTERVAL 1 MINUTES")) |>
    #     group_by(sensor_id, date = as.Date(time), hour = hour(time)) |>
    #     summarise(value = round(mean(value, na.rm = TRUE), 1), .groups = "drop_last") |>
    #     summarise(T_MIN = min(value, na.rm = TRUE), T_MAX = max(value, na.rm = TRUE), .groups = "drop") |>
    #     pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
    #     mutate(dataset = "ARPALombardia") |>
    #     to_arrow() |>
    #     compute()
    wider_spec <- tribble(
        ~.name, ~.value, ~idOperatore,
        "T_MIN", "value", 2L,
        "T_MAX", "value", 3L,
        "T_AVG", "value", 1L,
    )

    query_parquet(list.files(file.path(path.lom, "dataset"), full.names = TRUE), conn = dataconn) |>
        rename(sensor_id = station_id) |>
        mutate(sensor_id = as.integer(sensor_id), time = time - sql("INTERVAL 1 MINUTES")) |>
        semi_join(metadata, by = "sensor_id") |>
        filter(-35 < value, value < 50) |>
        group_by(sensor_id, idOperatore, date = as.Date(time), hour = hour(time)) |>
        summarise(value = mean(value, na.rm = TRUE), .groups = "drop") |>
        left_join(tibble(idOperatore = c(1L, 2L, 3L), variable = c("T_AVG", "T_MIN", "T_MAX")), by = "idOperatore", copy = TRUE) |>
        dbplyr_pivot_wider_spec(wider_spec, id_cols = c("sensor_id", "date", "hour"), values_fill = NA_real_) |>
        mutate(T_MIN = round(coalesce(T_AVG, (T_MAX + T_MIN) / 2, T_MIN, T_MAX), 1L), T_MAX = round(coalesce(T_AVG, (T_MAX + T_MIN) / 2, T_MAX, T_MIN), 1L)) |>
        group_by(sensor_id, date) |>
        summarise(T_MIN = min(T_MIN, na.rm = TRUE), T_MAX = max(T_MAX, na.rm = TRUE), .groups = "drop") |>
        pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
        mutate(dataset = "ARPALombardia") |>
        to_arrow() |>
        compute()
}

load_work_data.arpalombardia <- function(dataconn, metadata) {
    metadata <- metadata |> to_duckdb(con = dataconn)
    query_parquet(list.files(file.path(path.lom, "dataset"), full.names = TRUE), conn = dataconn) |>
        mutate(station_id = as.integer(station_id)) |>
        filter(!is.na(value)) |>
        rename(sensor_id = station_id) |>
        semi_join(metadata, by = "sensor_id") |>
        mutate(time = time - sql("INTERVAL 1 MINUTES")) |>
        group_by(sensor_id, date = as.Date(time)) |>
        summarise(
            T_MIN = min(value, na.rm = TRUE),
            T_MAX = max(value, na.rm = TRUE),
            .groups = "drop"
        ) |>
        pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
        mutate(dataset = "ARPALombardia") |>
        to_arrow() |>
        compute()
}

load_daily_data.arpalombardia <- function(dataconn, hexts = FALSE) {
    work_meta <- load_work_metadata.arpalombardia()
    if (hexts) {
        work_data <- load_hexts(dataconn, work_meta)
    } else {
        work_data <- load_work_data.arpalombardia(dataconn, work_meta)
    }
    work_data <- work_data |> # load_work_data.arpalombardia(work_meta) |>
        mutate(sensor_id = cast(sensor_id, utf8()))

    work_meta <- work_meta |>
        mutate(
            sensor_id = cast(sensor_id, utf8()),
            station_id = cast(station_id, utf8()),
            series_id = station_id
        ) |>
        compute()

    # work_meta <- work_meta |>
    #     semi_join(work_data, join_by(dataset, id == station_id)) |>
    #     compute()

    list(
        "meta" = work_meta,
        "data" = work_data
    )
}
