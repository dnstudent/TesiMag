library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")
source("src/analysis/data/quality_check.R")

dataset_spec <- function() {
    list(
        "https://dati.datamb.it/dataset/dati-dalle-stazioni-meteo-locali-della-rete-idrometeorologica-regionale/resource/d990dd69-61c1-41d2-802b-86dc293660a8",
        "regional",
        "Dataset di ARPAE. Portata istantanea. Sono stati utilizzati i dati aggregati qualora presenti, altrimenti sono stati aggregati a mano. L''orario è GMT, a differenza di altre regioni."
    )
}

load_daily_data.arpae <- function() {
    ds <- open_dataset(file.path(path.ds, "ARPA", "EMILIA-ROMAGNA", "OpenData", "tables")) |>
        # time is the start of the period in GMT
        mutate(name = cast(name, utf8()), network = cast(network, utf8()), time = (force_tz(date, "GMT") - as.difftime(agg_period, unit = "secs"))) |>
        select(-date) |>
        select(!c(P1, starts_with("level"))) |>
        mutate(station_id = str_c("-", str_c(lon, lat, sep = ","), network, sep = "/"), series_id = str_c(lon, lat, sep = ","))

    data <- ds |>
        select(station_id, time, T, variable, agg_period) |>
        mutate(value = T - 273.15, .keep = "unused") |>
        qc_gross() |>
        filter(qc_gross) |>
        select(-qc_gross) |>
        mutate(date = as.Date(time, tz = "GMT")) |>
        to_duckdb() |>
        group_by(station_id, date, agg_period) |>
        summarise(T_MIN = min(value, na.rm = TRUE), T_MAX = max(value, na.rm = TRUE), .groups = "drop_last") |>
        # Trusting goes: daily aggregates > hourly aggregates > instantaneous. This is because daily aggregates are computed on GMT day instead of CET, apparently.
        slice_max(agg_period) |>
        ungroup() |>
        pivot_longer(c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        mutate(dataset = "ARPAE") |>
        compute()

    dates_meta <- data |>
        group_by(dataset, station_id) |>
        summarise(station_first = min(date), station_last = max(date), .groups = "drop")

    md <- distinct(ds, station_id, series_id, name, network, lon, lat, elevation, WMO_block, WMO_num) |>
        mutate(lon = lon / 100000, lat = lat / 100000) |>
        mutate(dataset = "ARPAE", kind = "automatica") |>
        mutate(
            sensor_id = NA_character_,
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            town = NA_character_,
            user_code = NA_character_
        ) |>
        left_join(dates_meta, by = c("dataset", "station_id")) |>
        mutate(
            sensor_first = station_first,
            sensor_last = station_last
        ) |>
        compute()

    # md <- md |>
    #     semi_join(data, join_by(station_id)) |>
    #     compute()

    list("meta" = md, "data" = data)
}
