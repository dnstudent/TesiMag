library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(vroom, warn.conflicts = FALSE)
library(units, warn.conflicts = FALSE)

source("src/paths/paths.R")

dataset_spec <- function() {
    list(
        "https://www.ncei.noaa.gov/cdo-web/datasets/GHCND/locations/FIPS:IT/detail",
        "national",
        "Dataset GHCND (Global Historical Climatology Network - Daily) della NOAA. Registra i dati delle stazioni sinottiche dell''aeronautica militare italiana. Passo giornaliero, minime e massime effettive."
    )
}


path.sinottica <- file.path(path.ds, "NOAA_GHCND")

load_metadata <- function() {
    vroom_fwf(
        file.path(path.sinottica, "ghcnd-stations.txt"),
        fwf_cols(
            id = c(1, 11),
            lat = c(12, 20),
            lon = c(21, 30),
            elevation = c(31, 37),
            state = c(38, 40),
            name = c(41, 70),
            kind = c(71, 79),
            code = c(80, 85)
        ),
        col_types = "cdddccci"
    ) |>
        as_tibble() |>
        mutate(dataset = "NOAA_GHCND", network = "Sinottica", state = NA_character_)
}

load_data <- function(first_date, last_date) {
    data.sinottica <- open_csv_dataset(file.path(path.sinottica, "fragments"), na = c("", "NA", "9999")) |>
        rename(station_id = STATION, name = NAME, date = DATE) |>
        filter(first_date <= date & date <= last_date) |>
        select(station_id, date, starts_with("T")) |>
        collect() |>
        mutate(
            across(ends_with("ATTRIBUTES"), ~ str_split_fixed(., ",", 3) |>
                as_tibble() |>
                select(V2, V3) |>
                rename(QUALITY_FLAG = V2, SOURCE_FLAG = V3))
        ) |>
        rename(T_MIN = TMIN, T_MAX = TMAX, T_MAX_ATTRIBUTES = TMAX_ATTRIBUTES, T_MIN_ATTRIBUTES = TMIN_ATTRIBUTES)
    # Metadata were checked and found to be unique wrt the identifier-lon-lat-elevation tuple. These never change in station history, apparently.
    # metadata.sinottica <- select(dataset.sinottica, station_id, name, lon, lat, elevation)

    data.values <- data.sinottica |>
        select(station_id, date, T_MIN, T_MAX) |>
        pivot_longer(c(T_MIN, T_MAX), names_to = "variable", values_to = "value")

    data.flags <- data.sinottica |>
        select(date, station_id, ends_with("ATTRIBUTES")) |>
        pivot_longer(ends_with("ATTRIBUTES"), names_pattern = "(T_MAX|T_MIN)_ATTRIBUTES", names_to = "variable") |>
        unnest(value)

    left_join(data.values, data.flags, by = c("station_id", "variable", "date"), relationship = "one-to-one") |>
        filter(QUALITY_FLAG == "") |>
        drop_na(value) |>
        select(!c(QUALITY_FLAG, SOURCE_FLAG)) |>
        mutate(dataset = "NOAA_GHCNS")
    # left_join(data.sinottica |> select(!starts_with("T")), by = c("station_id", "date"), relationship = "many-to-one") |>
    # mutate(
    #     value = if_else(QUALITY_FLAG == "", value, NA)
    # )
}

load_daily_data.synop <- function(first_date, last_date) {
    data <- load_data(first_date, last_date)
    meta <- load_metadata() |>
        semi_join(data, join_by(id == station_id)) |>
        as_arrow_table()
    list("meta" = meta, "data" = data |> as_arrow_table())
}
