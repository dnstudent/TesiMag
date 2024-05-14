prepare_corrections <- function(raw_corrections) {
  corrections |>
    mutate(
      sensor_key = as.integer(sensor_key),
      from_datasets = str_split(from_datasets, regex(";|\\*")),
      from_sensor_keys = str_split(from_sensor_keys, regex(";|\\*")) |> purrr::map(as.integer),
      keep = coalesce(keep, TRUE),
      loc_correction = !is.na(lon_ok) | !is.na(lat_ok),
      elev_correction = !is.na(ele_ok),
      loc_precision = if_else(loc_correction, coalesce(loc_precision, -1), coalesce(loc_precision, 0)) |> as.integer(),
    ) |>
    mutate(
      elev_precision = if_else(elev_correction | (loc_precision == -1L), coalesce(elev_precision, -1), coalesce(elev_precision, 0)) |> as.integer()
    )
}

prepare_metadata <- function(merged_metadata, series_groups) {
  series_groups |>
    distinct(set, gkey, sensor_key, dataset, .keep_all = TRUE) |>
    select(set, gkey, sensor_key, dataset, rank) |>
    left_join(raw_metadata, by = c("dataset", "sensor_key")) |>
    group_by(set, gkey) |>
    summarise(
      name = first(name)
    )
}

merge_metadata <- function(raw_metadata, raw_corrections) {
  metadata |>
    full_join(prepare_corrections(raw_corrections), by = c("sensor_key", "dataset", "from_sensor_keys", "from_datasets")) |>
    assert(not_na, c(network, sensor_key, dataset, from_sensor_keys, from_datasets)) |>
    mutate(
      lon = coalesce(lon_ok, lon),
      lat = coalesce(lat_ok, lat),
      elevation = coalesce(ele_ok, elevation),
      name = coalesce(name_ok, name)
    )
}
