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
        mutate(station_name = cast(name, utf8()), network = cast(network, utf8()), time = (force_tz(date, "GMT") - as.difftime(agg_period, unit = "secs"))) |>
        select(-date) |>
        select(!c(P1, starts_with("level"), name)) |>
        mutate(original_id = str_c("-", str_c(lon, lat, sep = ","), network, sep = "/"))

    md <- distinct(ds, original_id, station_name, network, lon, lat, elevation, WMO_block, WMO_num) |>
        mutate(lon = lon / 100000, lat = lat / 100000) |>
        mutate(state = "Emilia-Romagna", original_dataset = "ARPAE", kind = "automatica") |>
        rename(name = station_name) |>
        compute()

    data <- ds |>
        select(station_id = original_id, time, T, variable, agg_period) |>
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

    md <- md |>
        semi_join(data, join_by(original_id == station_id)) |>
        compute()

    list("meta" = md, "data" = data)
}
