library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(stars, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/load/tools.R")
source("src/analysis/metadata.R")
source("src/database/definitions.R")
source("src/database/tools.R")

dataset_spec <- function() {
    list(
        "https://dati.regione.umbria.it/dataset/sir_termometro_aria_storico/resource/bb38b77a-b5a0-46c1-ac99-997642c42639",
        "regional",
        "Dataset di ARPA Umbria. Passo giornaliero. Recuperato da OpenData."
    )
}

load_daily_data.umbria <- function(first_date, last_date) {
    full_table <- read_csv_arrow(
        file.path(path.ds, "ARPA", "UMBRIA", "rilevazioni_temperatura_aria_storico_dati_giornalieri.csv"),
        skip = 1L,
        schema = schema(
            ID_SENSORE_DETTAGLIO = int32(),
            ID_TIPOLOGIA_SENSORE = int32(),
            STRUMENTO = utf8(),
            TIPO_STRUMENTO = utf8(),
            UNITA_MISURA = utf8(),
            ID_STAZIONE = int32(),
            COMUNE = utf8(),
            NOME_STAZIONE = utf8(),
            LATITUDINE = float64(),
            LONGITUDINE = float64(),
            ANNO = int32(),
            MESE = int32(),
            GIORNO = int32(),
            AVGDAY = float64(),
            MINDAY = float64(),
            MAXDAY = float64()
        ),
        as_data_frame = FALSE
    ) |>
        mutate(date = make_date(ANNO, MESE, GIORNO)) |>
        rename(station_id = ID_STAZIONE, name = NOME_STAZIONE, lat = LATITUDINE, lon = LONGITUDINE, T_MIN = MINDAY, T_MAX = MAXDAY) |>
        select(!c(ANNO, MESE, GIORNO, AVGDAY)) |>
        filter(first_date <= date & date <= last_date)

    meta <- full_table |>
        select(!c(T_MIN, T_MAX, date)) |>
        distinct() |>
        mutate(
            state = "Umbria",
            dataset = "ARPAUmbria",
            network = "ARPAUmbria",
        ) |>
        rename(id = station_id) |>
        collect() |>
        st_md_to_sf(remove = FALSE) |>
        add_dem_elevations(dem = read_stars(file.path("temp", "dem", "dem30.tif"))) |>
        st_drop_geometry() |>
        rename(elevation = dem)

    data <- full_table |>
        select(station_id, date, T_MIN, T_MAX) |>
        to_duckdb() |>
        pivot_longer(c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        filter(!is.na(value)) |>
        mutate(dataset = "ARPAUmbria") |>
        to_arrow() |>
        as_arrow_table2(data_schema)

    list("meta" = meta, "data" = data)
}
