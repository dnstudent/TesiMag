library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/paths/paths.R")
source("src/analysis/data/quality_check.R")

path.lom <- file.path(path.ds, "ARPA", "LOMBARDIA")

load_work_metadata.arpal <- function() {
    path.md <- file.path(path.lom, "Stazioni_Meteorologiche.tsv")
    read_tsv_arrow(path.md,
        col_types = schema(
            IdSensore = uint32(),
            IdStazione = utf8(),
            NomeStazione = utf8(),
            Tipologia = utf8(),
            Provincia = utf8(),
            DataStart = timestamp(unit = "s"),
            DataStop = timestamp(unit = "s"),
            Quota = double(),
            lng = double(),
            lat = double()
        ),
        timestamp_parsers = "%d/%m/%Y",
        as_data_frame = FALSE,
        col_select = c("IdSensore", "IdStazione", "NomeStazione", "Tipologia", "Provincia", "DataStart", "DataStop", "Quota", "lng", "lat")
    ) |>
        filter(Tipologia == "Temperatura") |>
        mutate(across(starts_with("Data"), ~ cast(., date32())), network = "ARPALombardia", state = "Lombardia", dataset_id = "ARPALombardia") |>
        rename(
            original_id = IdSensore,
            station_name = NomeStazione,
            province = Provincia,
            elevation = Quota,
            lon = lng,
            type = Tipologia,
            original_station_id = IdStazione,
            first_date = DataStart,
            last_date = DataStop
        )
}

open_orig_ds.arpal <- function(metadata) {
    path.fragments <- file.path(path.lom, "fragments")

    read_conf <- list(
        "schema" = input_csv_schema <- schema(
            IdSensore = uint32(),
            Data = timestamp(unit = "s"),
            Valore = float64(),
            Stato = utf8(),
            idOperatore = uint32()
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
        rename(original_id = IdSensore, time = Data, value = Valore) |>
        filter(as.Date("2000-01-01") <= as.Date(time) & as.Date(time) <= as.Date("2022-12-31")) |>
        semi_join(metadata, by = "original_id") |>
        # write_dataset(file.path(path.lom, "cache"))
        # open_dataset(file.path(path.lom, "cache")) |>
        mutate(time = time - as.difftime(10 * 60, unit = "secs"))
}

load_daily_data.arpal <- function() {
    work_meta <- load_work_metadata.arpal()
    work_data <- open_orig_ds.arpal(work_meta) |>
        gross_errors_check(value, thresh = 50) |>
        group_by(original_id, date = date(time)) |>
        summarise(
            T_MIN = min(value, na.rm = TRUE),
            T_MAX = max(value, na.rm = TRUE),
            any_gross = any(qc_gross, na.rm = TRUE),
            .groups = "drop"
        ) |>
        filter(!any_gross) |>
        select(-any_gross) |>
        mutate(original_id = cast(original_id, utf8())) |>
        collect() |>
        pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
        mutate(merged = FALSE) |>
        as_arrow_table()

    work_meta <- work_meta |>
        collect() |>
        mutate(original_id = as.character(original_id)) |>
        name_stations() |>
        as_arrow_table()

    work_data <- work_data |>
        left_join(work_meta |> select(original_id, station_id), by = "original_id") |>
        select(-original_id) |>
        compute()

    list(
        "meta" = work_meta,
        "data" = work_data
    )
}
