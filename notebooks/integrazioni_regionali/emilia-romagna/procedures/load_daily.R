library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/database/tools.R")
source("src/analysis/data/quality_check.R")

load_daily_data.arpae <- function() {
    ds <- open_dataset(file.path(path.ds, "ARPA", "EMILIA-ROMAGNA", "tables"), format = "feather") |>
        mutate(station_name = cast(name, utf8()), network = cast(network, utf8()), date = date - as.difftime(agg_period, unit = "secs")) |>
        select(!c(P1, starts_with("level"), name)) |>
        mutate(original_id = str_c(network, station_name, elevation, sep = "/"))

    md <- distinct(ds, original_id, station_name, network, lon, lat, elevation, WMO_block, WMO_num) |>
        mutate(lon = lon / 100000, lat = lat / 100000) |>
        collect() |>
        mutate(state = "Emilia-Romagna", dataset_id = "ARPAE") |>
        assert(is_uniq, original_id) |>
        name_stations() |>
        as_arrow_table()

    data <- ds |>
        left_join(md |> select(original_id, station_id), by = "original_id") |>
        select(station_id, date, T, variable, agg_period) |>
        mutate(T = T - 273.15) |>
        gross_errors_check(T) |>
        group_by(station_id, date = as.Date(date)) |>
        summarise(T_MIN = min(T, na.rm = TRUE), T_MAX = max(T, na.rm = TRUE), qc_gross = any(qc_gross)) |>
        ungroup() |>
        filter(!qc_gross) |>
        arrange(station_id, date) |>
        collect() |>
        pivot_longer(c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        arrange(station_id, variable, date) |>
        select(!starts_with("qc")) |>
        mutate(merged = FALSE) |>
        as_arrow_table2(data_schema)

    list("meta" = md, "data" = data)
}
