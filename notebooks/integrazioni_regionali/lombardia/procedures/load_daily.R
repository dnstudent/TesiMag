library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(duckdb, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

source("src/database/tools.R")
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
        col_types = schema(
            IdSensore = int32(),
            IdStazione = utf8(),
            NomeStazione = utf8(),
            Tipologia = utf8(),
            Provincia = utf8(),
            DataStart = timestamp(unit = "s"),
            DataStop = timestamp(unit = "s"),
            Quota = float64(),
            lng = float64(),
            lat = float64()
        ),
        timestamp_parsers = "%d/%m/%Y",
        as_data_frame = FALSE,
        col_select = c("IdSensore", "IdStazione", "NomeStazione", "Tipologia", "Provincia", "DataStart", "DataStop", "Quota", "lng", "lat")
    ) |>
        filter(Tipologia == "Temperatura") |>
        mutate(across(starts_with("Data"), ~ cast(., date32())), network = "ARPALombardia", state = "Lombardia", dataset = "ARPALombardia") |>
        rename(
            id = IdSensore,
            name = NomeStazione,
            province = Provincia,
            elevation = Quota,
            lon = lng,
            type = Tipologia,
            original_station_id = IdStazione,
            first_date = DataStart,
            last_date = DataStop
        ) |>
        compute()
}

open_orig_ds.arpalombardia <- function(metadata) {
    path.fragments <- file.path(path.lom, "fragments")

    read_conf <- list(
        "schema" = schema(
            IdSensore = int32(),
            Data = timestamp(unit = "s"),
            Valore = float64(),
            Stato = utf8(),
            idOperatore = int32()
        ),
        "ts_parsers" = csv_timestamp_parsers <- c("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S.000"),
        "nas" = na_strings <- c("", "NA", "9999.0", "-9999.0", "-9999", "NV", "NC", "ND")
    )

    open_csv_dataset(fs::dir_ls(path.fragments, glob = "*.csv"),
        hive_style = FALSE,
        schema = read_conf$schema,
        timestamp_parsers = read_conf$ts_parsers,
        na = read_conf$nas,
        col_names = TRUE,
        skip = 1
    ) |>
        rename(station_id = IdSensore, time = Data, value = Valore) |>
        semi_join(metadata, join_by(station_id == id)) |>
        # write_dataset(file.path(path.lom, "cache"))
        # open_dataset(file.path(path.lom, "cache")) |>
        mutate(time = time - as.difftime(10 * 60, unit = "secs"))
}

load_work_data.arpalombardia <- function(first_date, last_date, metadata) {
    conn <- dbConnect(duckdb(), file.path(path.lom, "lombardia.db"), read_only = TRUE)
    metadata <- metadata |> to_duckdb(con = conn, table_name = "metadata_tmp", auto_disconnect = FALSE)
    data <- tbl(conn, "full_tmp") |>
        semi_join(metadata, join_by(station_id == id)) |>
        gross_errors_check(value, thresh = 50) |>
        group_by(station_id, date = as.Date(time)) |>
        summarise(
            T_MIN = min(value, na.rm = TRUE),
            T_MAX = max(value, na.rm = TRUE),
            all_good = all(qc_gross, na.rm = TRUE),
            .groups = "drop"
        ) |>
        filter(all_good, first_date <= date & date <= last_date) |>
        select(-all_good) |>
        pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
        mutate(dataset = "ARPALombardia") |>
        filter(!is.na(value)) |>
        to_arrow() |>
        compute()
    data
}

load_daily_data.arpalombardia <- function(first_date, last_date) {
    work_meta <- load_work_metadata.arpalombardia()
    work_data <- load_work_data.arpalombardia(first_date, last_date, work_meta)

    work_meta <- work_meta |>
        semi_join(work_data, join_by(dataset, id == station_id)) |>
        compute()

    list(
        "meta" = work_meta,
        "data" = work_data
    )
}
