library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station = (overlap_union > 0.8 & f0 > 0.8),
            tag_same_series = !(name_x == "Vallombrosa" & name_y == "Vallombrosa") &
                (distance < 500 | (strSym == 1 & valid_days_inters == 0L & distance < 1000)),
            tag_mergeable = TRUE,
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series = tag_same_series & !(sensor_key_x == 160L & sensor_key_y == 369L),
        )
}
