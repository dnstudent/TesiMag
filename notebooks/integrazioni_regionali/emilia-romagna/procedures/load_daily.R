library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)

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

load_daily_data.dext3r <- function(dataconn) {
    meta <- read_parquet(file.path(path.ds, "ARPA", "EMILIA-ROMAGNA", "Dext3r", "stations.parquet"), as_data_frame = TRUE) |>
        mutate(dataset = "Dext3r", kind = "unknown") |>
        select(-geometry, -ident) |>
        unnest(cols = c(region, province, municipality, basin, subbasin, macroarea, country, owner, manager), names_sep = "_") |>
        hoist(categories, category = 1L) |>
        select(-categories) |>
        rename(elevation = height, state_o = region_name, province_full = province_name, province_num = province_code, station_id = id, town = municipality_name) |>
        mutate(
            lon = lon / 1e5,
            lat = lat / 1e5,
            province_full = str_to_title(province_full),
            series_id = station_id,
            sensor_id = station_id,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            user_code = NA_character_
        )

    data <- open_dataset(file.path(path.ds, "ARPA", "EMILIA-ROMAGNA", "Dext3r", "data", "fragments") |> list.files(pattern = "*.parquet", full.names = TRUE)) |>
        to_duckdb(con = dataconn) |>
        filter(!is.na(value)) |>
        group_by(id, variable, date = as.Date(start)) |>
        slice_min(start, with_ties = FALSE) |>
        ungroup() |>
        select(station_id = id, variable, date, value) |>
        mutate(dataset = "Dext3r") |>
        to_arrow() |>
        compute()

    list("meta" = meta, "data" = data)
}

load_data <- function() {
    ds <- open_dataset(file.path(path.ds, "ARPA", "EMILIA-ROMAGNA", "OpenData", "tables")) |>
        # time is the start of the period in GMT
        mutate(name = cast(name, utf8()), network = cast(network, utf8()), time = (force_tz(date, "GMT") - as.difftime(agg_period, unit = "secs"))) |>
        select(-date) |>
        select(!c(P1, starts_with("level"))) |>
        mutate(station_id = str_c("-", str_c(lon, lat, sep = ","), network, sep = "/"), series_id = str_c(lon, lat, sep = ",")) |>
        select(station_id, time, T, variable, agg_period) |>
        mutate(value = T - 273.15, .keep = "unused") |>
        mutate(date = as.Date(time + as.difftime(3600, units = "secs")), dataset = "ARPAE") |>
        filter(agg_period != 86400L, !is.na(value))
    # tmin <- ds |>
    #     filter(variable == 3L) |>
    #     select(dataset, station_id, date, value)
    # tmax <- ds |>
    #     filter(variable == 2L) |>
    #     select(dataset, station_id, date, value)
    concat_tables(tmin |> mutate(variable = "T_MIN") |> compute(), tmax |> mutate(variable = "T_MAX") |> compute())
}

load_daily_data.arpae <- function() {
    ds <- open_dataset(file.path(path.ds, "ARPA", "EMILIA-ROMAGNA", "OpenData", "tables")) |>
        # time is the start of the period in GMT
        mutate(name = cast(name, utf8()), network = cast(network, utf8()), time = (force_tz(date, "GMT") - as.difftime(agg_period, unit = "secs"))) |>
        select(-date) |>
        select(!c(P1, starts_with("level"))) |>
        mutate(station_id = str_c("-", str_c(lon, lat, sep = ","), network, sep = "/"), series_id = str_c(lon, lat, sep = ","))

    agg_prios <- arrow_table(agg_period = c(86400L, 3600L, 900L, 0L), priority = c(4L, 1L, 3L, 2L), schema = schema(agg_period = int64(), priority = int32()))

    data <- ds |>
        select(station_id, time, T, variable, agg_period) |>
        mutate(value = T - 273.15, .keep = "unused") |>
        filter(!is.na(value)) |>
        # qc_gross() |>
        # filter(qc_gross, agg_period != 86400L) |>
        # select(-qc_gross) |>
        mutate(date = as.Date(time, tz = "GMT")) |>
        group_by(station_id, date, agg_period) |>
        summarise(T_MIN = min(value, na.rm = TRUE), T_MAX = max(value, na.rm = TRUE), .groups = "drop_last") |>
        left_join(agg_prios, by = "agg_period") |>
        ungroup() |>
        to_duckdb() |>
        group_by(station_id, date) |>
        slice_min(priority) |>
        select(-priority) |>
        pivot_longer(c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        mutate(dataset = "ARPAE") |>
        compute()

    # data <- load_data()

    md <- distinct(ds, station_id, series_id, name, network, lon, lat, elevation, WMO_block, WMO_num) |>
        mutate(lon = lon / 100000, lat = lat / 100000) |>
        mutate(dataset = "ARPAE", kind = "automatica") |>
        mutate(
            sensor_id = NA_character_,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            town = NA_character_,
            user_code = NA_character_
        ) |>
        compute()

    # md <- md |>
    #     semi_join(data, join_by(station_id)) |>
    #     compute()

    list("meta" = md, "data" = data)
}
