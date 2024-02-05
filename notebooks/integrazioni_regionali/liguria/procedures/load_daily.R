library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/paths/paths.R")

dataset_spec <- function() {
    list(
        "https://ambientepub.regione.liguria.it/SiraQualMeteo/script/PubAccessoDatiMeteo.asp",
        "regional",
        "Dataset di ARPA Liguria. Misure giornaliere aggregate su orari UTC (?)"
    )
}

manual_corrections <- function(meta) {
    meta |> mutate(
        # Le serie di LOANO hanno la stessa anagrafica ma sono diverse. MATRA ha un'anagrafica strana, ma è MTRAN
        series_id = case_match(user_code, "LOAN0" ~ "LOANO", "VERZI" ~ "VERZI", "MATRA" ~ "MTRAN", .default = series_id),
    )
}

load_meta <- function() {
    read_csv_arrow(
        file.path(path.ds, "ARPA", "LIGURIA", "metadata.csv"),
        schema = schema(
            user_code = utf8(),
            anagrafica = utf8(),
            province = utf8(),
            BACINO = utf8(),
            elevation = float64(),
            identifier = utf8(),
            lon = float64(),
            lat = float64()
        ),
        skip = 1L
    ) |>
        mutate(
            network = "ARPAL",
            dataset = "ARPAL",
            kind = "unknown",
            province = str_to_title(province),
            station_id = str_c(anagrafica, trunc(lon * 10e4) * 10L, trunc(lat * 10e4) * 10L, sep = "_"),
            series_id = anagrafica,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            town = NA_character_,
        ) |>
        manual_corrections() |>
        rename(province_full = province, name = anagrafica, sensor_id = identifier) |>
        as_arrow_table()
}

load_data <- function() {
    open_dataset(file.path(path.ds, "ARPA", "LIGURIA", "dataset")) |>
        # VESSA è il codice della stazione di BARCHEO, che fornisce 134 giorni di dati duplicati con RANZO
        filter(valid, user_code != "VESSA") |>
        select(!c(anagrafica, Dataset, valid)) |>
        mutate(dataset = "ARPAL", user_code = cast(user_code, utf8())) |>
        compute()
}

load_daily_data.arpal <- function() {
    data <- load_data()
    meta <- load_meta() |>
        semi_join(data |> distinct(user_code) |> compute(), by = "user_code") |>
        compute()

    list("meta" = meta, "data" = data)
}
