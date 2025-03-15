library(dplyr, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(openxlsx, warn.conflicts = FALSE)

source("src/database/query/spatial.R")

load_corrections <- function(from_path) {
  from_path |>
    fs::dir_ls(regex = regex("^[^\\~]+_edit.xlsx")) |>
    purrr::map(
      .f = \(path) read.xlsx(path) |>
        select(dataset, from_sensor_keys, from_datasets, ends_with("_ok"), ends_with("_precision"), keep, note) |>
        mutate(
          across(
            c(dataset, note), ~ as.character(.)
          ),
          across(
            c(lon_ok, lat_ok, ele_ok, ends_with("_precision")), ~ as.numeric(.)
          )
        )
    ) |>
    bind_rows()
}

prepare_corrections <- function(corrections, metadata) {
  corrections |>
    mutate(
      from_datasets = str_split(from_datasets, regex(";|\\*")),
      from_sensor_keys = str_split(from_sensor_keys, regex(";|\\*")) |> purrr::map(as.integer)
    ) |>
    right_join(metadata |> select(dataset, from_datasets, from_sensor_keys, series_last, valid90), by = c("dataset", "from_datasets", "from_sensor_keys"), relationship = "one-to-one") |>
    mutate(
      manual_loc_correction = !is.na(lon_ok) | !is.na(lat_ok),
      manual_elev_correction = !is.na(ele_ok),
      loc_precision = coalesce(if_else(manual_loc_correction, coalesce(loc_precision, -1), coalesce(loc_precision, if_else(year(series_last) <= 2010L, 1, 0))), 1),
      elev_precision = coalesce(if_else(manual_elev_correction | (loc_precision == -1), coalesce(elev_precision, -1), coalesce(elev_precision, if_else(year(series_last) <= 2010L, 1, 0))), 1),
      keep = coalesce(keep, valid90 >= 5L * 365L, FALSE)
    ) |>
    assert(within_bounds(3, 19), lon_ok) |>
    assert(within_bounds(41, 49), lat_ok) |>
    assert(within_bounds(-10, 4900), ele_ok) |>
    select(-series_last, -valid90)
}


integrate_corrections <- function(merged_metadata, raw_corrections, statconn) {
  corrections <- prepare_corrections(raw_corrections, merged_metadata)
  merged_metadata |>
    full_join(corrections, by = c("from_sensor_keys", "from_datasets", "dataset"), relationship = "one-to-one") |>
    assert(not_na, c(dataset, network, from_sensor_keys, from_datasets)) |>
    mutate(
      lon = coalesce(lon_ok, lon),
      lat = coalesce(lat_ok, lat),
      elevation = coalesce(ele_ok, elevation),
      name = coalesce(name_ok, name),
      user_code = coalesce(user_code_ok, user_code),
      .keep = "unused"
    ) |>
    select(-elevation_glo30) |>
    rename(sensor_key = series_key) |>
    query_elevations(statconn) |>
    mutate(elevation = coalesce(elevation, elevation_glo30)) |>
    rename(series_key = sensor_key)
}
