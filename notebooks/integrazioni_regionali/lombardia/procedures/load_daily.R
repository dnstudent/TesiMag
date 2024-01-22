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
        schema = schema(
            IdSensore = int32(),
            Tipologia = utf8(),
            `Unità DiMisura` = utf8(),
            IdStazione = utf8(),
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
        mutate(across(starts_with("Data"), ~ cast(., date32())), network = "ARPALombardia", state = "Lombardia", original_dataset = "ARPALombardia", kind = "unknown") |>
        rename(
            original_id = IdSensore,
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

load_work_data.arpalombardia <- function(metadata) {
    conn <- dbConnect(duckdb())
    metadata <- metadata |> to_duckdb(con = conn)
    suppressMessages(tbl(conn, "read_parquet('/Users/davidenicoli/Local_Workspace/Datasets/ARPA/LOMBARDIA/dataset/*.parquet')")) |>
        semi_join(metadata, join_by(station_id == original_id)) |>
        mutate(time = time - sql("INTERVAL 10 MINUTES")) |>
        qc_gross(threshold = 50) |>
        group_by(station_id, date = as.Date(time)) |>
        filter(all(qc_gross, na.rm = TRUE)) |>
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

load_daily_data.arpalombardia <- function() {
    work_meta <- load_work_metadata.arpalombardia()
    work_data <- load_work_data.arpalombardia(work_meta)

    # work_meta <- work_meta |>
    #     semi_join(work_data, join_by(dataset, id == station_id)) |>
    #     compute()

    list(
        "meta" = work_meta,
        "data" = work_data
    )
}
