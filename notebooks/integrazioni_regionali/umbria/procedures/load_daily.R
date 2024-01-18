library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(stars, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/load/tools.R")
source("src/database/tools.R")

dataset_spec <- function() {
    list(
        "https://dati.regione.umbria.it/dataset/sir_termometro_aria_storico/resource/bb38b77a-b5a0-46c1-ac99-997642c42639",
        "regional",
        "Dataset di ARPA Umbria. Passo giornaliero. Recuperato da OpenData."
    )
}

load_meta <- function() {
    dem <- read_stars(file.path("temp", "dem", "dem30.tif"))
    vroom::vroom(file.path(path.ds, "ARPA", "UMBRIA", "elenco_stazioni.csv"), show_col_types = FALSE, col_types = "iicddccc") |>
        as_tibble() |>
        select(-`_id`) |>
        rename(original_id = ID_STAZIONE, name = NOME_STAZIONE, lat = LAT, lon = LON) |>
        mutate(
            state = "Umbria",
            original_dataset = "ARPAUmbria",
            network = "ARPAUmbria",
            kind = "unknown"
        ) |>
        mutate(elevation = st_extract(dem, pick(lon, lat) |> as.matrix()) |> pull(1)) |>
        as_arrow_table()
}

load_daily_data.umbria <- function(first_date, last_date) {
    data <- read_csv_arrow(
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
        col_select = c("ID_STAZIONE", "ANNO", "MESE", "GIORNO", "MINDAY", "MAXDAY"),
        as_data_frame = FALSE
    ) |>
        mutate(date = make_date(ANNO, MESE, GIORNO), .keep = "unused") |>
        rename(station_id = ID_STAZIONE, T_MIN = MINDAY, T_MAX = MAXDAY) |>
        to_duckdb() |>
        pivot_longer(c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        filter(!is.na(value)) |>
        mutate(dataset = "ARPAUmbria") |>
        to_arrow() |>
        compute()

    meta <- load_meta()

    list("meta" = meta, "data" = data)
}
