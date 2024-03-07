library(dplyr, warn.conflicts = FALSE)

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series =
                (dataset_x == "ARPAL" & dataset_y == "ARPAL" & sensor_key_x == 62L & sensor_key_y == 63L) |
                    (
                        tag_same_series &
                            !(user_code_x == "VERZI" & user_code_y == "07LOAN0") &
                            !(dataset_x == "SCIA" & dataset_y == "SCIA" &
                                (
                                    (name_x == "Calice Ligure" & name_y == "Calice Ligure") |
                                        (sensor_key_x == 1803L & sensor_key_y == 3118L) | # Mattarana - Tavarone
                                        (sensor_key_x == 1331L & sensor_key_y == 1333L) # Giacopiane Diga / Lago
                                )
                            ) &
                            !(dataset_x == "ARPAL" & dataset_y == "ARPAL" &
                                (sensor_key_x == 22L & sensor_key_y == 23L)
                            ) &
                            !(dataset_x == "ARPAL" & dataset_y == "SCIA" &
                                (sensor_key_x == 22L & sensor_key_y == 687L)
                            )
                    )
        )
}

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.8),
            tag_same_station = (str_detect(user_code_y, user_code_x) | (overlap_min > 0.8 & f0 > 0.8)),
            tag_same_series = !(network_x %in% c("Sinottica", "Mareografica")) &
                !(network_y %in% c("Sinottica", "Mareografica")) &
                !(dataset_x == "ARPAL" & dataset_y == "ARPAL" & distance > 4000) &
                !(dataset_x == "ARPAL" & dataset_y == "SCIA" & valid_days_inters < 5L & !distance < 10) &
                (
                    (valid_days_inters >= 160L & f0 > 0.14) |
                        (valid_days_inters < 160L & (distance < 560 | strSym > 0.92) & (valid_days_inters < 10L | f0 > 0.05))
                ),
        )
}

tag_mergeable <- function(analysis) {
    analysis |>
        mutate(
            tag_mergeable = (valid_days_inters < 90L) | (!is.na(monthlymaeT) & monthlymaeT <= 0.5),
        )
}
