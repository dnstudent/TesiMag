library(dplyr, warn.conflicts = FALSE)

default_logic <- function(tagged_analysis, require_all) {
    tagged_analysis |>
        mutate(
            tag_same_sensor = ((dataset_x == dataset_y) & (sensor_key_x == sensor_key_y)) | tag_same_sensor,
            tag_same_station = ((dataset_x == dataset_y) & (station_key_x == station_key_y)) | tag_same_station | tag_same_sensor,
            tag_same_series = ((dataset_x == dataset_y) & (series_key_x == series_key_y)) | tag_same_series | tag_same_station | tag_same_sensor,
        ) |>
        group_by(key_x, key_y) |>
        mutate(
            tag_same_sensor = if_else(require_all, all(tag_same_sensor), any(tag_same_sensor)),
            tag_same_station = if_else(require_all, all(tag_same_station), any(tag_same_station)),
            tag_same_series = if_else(require_all, all(tag_same_series), any(tag_same_series)),
        ) |>
        ungroup() |>
        mutate(
            tag_mergeable = tag_same_series & tag_mergeable
        )
}
