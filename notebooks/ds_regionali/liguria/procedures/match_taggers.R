library(dplyr, warn.conflicts = FALSE)

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series = tag_same_series &
                !(user_code_x == "VERZI" & user_code_y == "07LOAN0") &
                !(dataset_x == "SCIA" & dataset_y == "SCIA" &
                    (
                        (sensor_key_x == 1803L & sensor_key_y == 3118L) | # Mattarana / Tavarone
                            (sensor_key_x == 1803L & sensor_key_y == 3119L) | # Mattarana / Tavarone
                            (sensor_key_x == 1331L & sensor_key_y == 1333L) | # Giacopiane Diga / Lago
                            (sensor_key_x == 681L & sensor_key_y == 2083L) | # Cairo Montenotte / Osiglia
                            (sensor_key_x == 112L & sensor_key_y == 1867L) # Albenga / Molino Branca
                    )
                ) |
                ((dataset_x == "SCIA" & dataset_y == "SCIA") & (sensor_key_x == 29L & sensor_key_y == 126L))
        )
}

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.8),
            tag_same_station = (str_detect(user_code_y, user_code_x) | (overlap_min > 0.8 & f0 > 0.8)),
            # Same series
            tag_sseries_ava = (dataset_x == "ARPAL" & dataset_y == "ARPAL") & (
                distance < 600
            ),
            tag_sseries_svs = (dataset_x == "SCIA" & dataset_y == "SCIA") & (
                distance <= 620
            ),
            tag_sseries_avs = (dataset_x == "ARPAL" & dataset_y == "SCIA") & (
                (valid_days_inters >= 160L & f0 > 0.1) |
                    (valid_days_inters < 160L & (distance < 300 | strSym > 0.92 | (distance < 600 & abs(delH) < 10)))
            ),
            tag_sseries_ivs = (network_x == "ISAC" & dataset_y == "SCIA") & (
                !is.na(user_code_y) & user_code_x == user_code_y
            ),
            tag_synopok = (network_y != "Sinottica" | (network_x == "ISAC" & network_y == "Sinottica")) & (network_x != "Sinottica"),
            tag_same_series = (tag_sseries_ava | tag_sseries_svs | tag_sseries_avs | tag_sseries_ivs) & tag_synopok,
        )
}

tag_mergeable <- function(analysis) {
    analysis |>
        mutate(
            tag_mergeable = (valid_days_inters < 90L) | (!is.na(monthlymaeT) & monthlymaeT <= 0.5),
        )
}
