library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_sseries_ava = (dataset_x == "ARPALombardia" & dataset_y == "ARPALombardia") & FALSE,
            tag_sseries_avs = (dataset_x == "ARPALombardia" & dataset_y == "SCIA") & (
                (user_code_x == user_code_y) |
                    (valid_days_inters >= 100L & f0 > 0.9) |
                    (valid_days_inters < 100L & strSym > 0.92 & (is.na(delH) | abs(delH < 150))) |
                    (distance < 350 & (is.na(delH) | abs(delH < 150)))
            ),
            tag_sseries_svs = (dataset_x == "SCIA" & dataset_y == "SCIA") & (
                distance < 1000 & (is.na(delH) | abs(delH) < 150)
            ),
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station = (overlap_min > 0.9 & f0 > 0.9),
            tag_same_series = tag_sseries_avs | tag_sseries_svs
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series = tag_same_series &
                !((dataset_x == "ARPALombardia" & dataset_y == "SCIA") & (
                    (sensor_key_x == 193L & sensor_key_y == 1416L) |
                        (sensor_key_x == 230L & sensor_key_y == 2733L) |
                        (sensor_key_x == 114L & (sensor_key_y == 3341L | sensor_key_y == 3342L)) |
                        (sensor_key_x == 13L & sensor_key_y == 902L)
                ) |
                    (dataset_x == "SCIA" & dataset_y == "SCIA") & (
                        (sensor_key_x == 902L & sensor_key_y == 903L)
                    )
                )
        )
}
