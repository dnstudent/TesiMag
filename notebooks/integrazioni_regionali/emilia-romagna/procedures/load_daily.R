library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")
source("src/analysis/data/quality_check.R")

dataset_spec <- function() {
    list(
        "https://dati.datamb.it/dataset/dati-dalle-stazioni-meteo-locali-della-rete-idrometeorologica-regionale/resource/d990dd69-61c1-41d2-802b-86dc293660a8",
        "regional",
        "Dataset di ARPAE. Passo suborario. Sono stati utilizzati i dati aggregati qualora presenti, altrimenti sono stati aggregati a mano."
    )
}

load_daily_data.arpae <- function(first_date, last_date) {
    ds <- open_dataset(file.path(path.ds, "ARPA", "EMILIA-ROMAGNA", "tables"), format = "feather") |>
        mutate(station_name = cast(name, utf8()), network = cast(network, utf8()), date = date - as.difftime(agg_period, unit = "secs")) |>
        select(!c(P1, starts_with("level"), name)) |>
        mutate(original_id = str_c(network, station_name, elevation, sep = "/"))

    md <- distinct(ds, original_id, station_name, network, lon, lat, elevation, WMO_block, WMO_num) |>
        mutate(lon = lon / 100000, lat = lat / 100000) |>
        collect() |>
        mutate(state = "Emilia-Romagna", dataset = "ARPAE") |>
        rename(id = original_id, name = station_name) |>
        assert(is_uniq, id) |>
        as_arrow_table()

    data <- ds |>
        select(station_id = original_id, date, T, variable, agg_period) |>
        mutate(T = T - 273.15) |>
        gross_errors_check(T) |>
        group_by(station_id, date = as.Date(date)) |>
        summarise(T_MIN = min(T, na.rm = TRUE), T_MAX = max(T, na.rm = TRUE), qc_gross = any(qc_gross)) |>
        ungroup() |>
        filter(qc_gross, first_date <= date & date <= last_date) |>
        arrange(station_id, date) |>
        to_duckdb() |>
        pivot_longer(c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        arrange(station_id, variable, date) |>
        select(!starts_with("qc_")) |>
        mutate(dataset = "ARPAE") |>
        to_arrow() |>
        compute()

    md <- md |>
        semi_join(data, join_by(dataset, id == station_id)) |>
        compute()

    list("meta" = md, "data" = data)
}
