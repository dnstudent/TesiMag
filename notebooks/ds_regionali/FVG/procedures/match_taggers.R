library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_extavgs = (
                dataset_x == "ARPAFVG" & valid_days_inters > 160L & abs(balance) > 0.8 & !is.na(strSym) & strSym > 0.8
            ),
            tag_dist = (
                ((valid_days_inters < 160 & distance < 600) | (valid_days_inters >= 160 & monthlymaeT < 0.65 & distance < 250))
            ),
            tag_ava = (dataset_x == "ARPAFVG" & dataset_y == "ARPAFVG") & (
                distance < 50
            ),
            tag_avs = (dataset_x == "ARPAFVG" & dataset_y == "SCIA") & (
                distance <= 626L & network_y != "Sinottica"
            ),
            tag_avi = (dataset_x == "ARPAFVG" & dataset_y == "ISAC") & (
                network_y == "DPC" & distance < 1000 & (is.na(delH) | abs(delH) < 100) & ((valid_days_inters < 160 & strSym > 0.95) | monthlymaeT < 0.8)
            ),
            tag_ivs = (dataset_x == "ISAC" & dataset_y == "SCIA") & (
                (valid_days_inters >= 160L & f0 > 0.1) | (valid_days_inters < 160L & (!is.na(strSym) & strSym > 0.9)) & (network_y != "Sinottica" | (network_x == "ISAC" & network_y == "Sinottica"))
            ),
            tag_ivi = (dataset_x == "ISAC" & dataset_y == "ISAC") & (
                (!is.na(f0) & f0 > 0.12) | (valid_days_inters < 160L & strSym > 0.9)
            ),
            tag_oksynop = (network_x != "Sinottica" & network_y != "Sinottica") | (network_x == "Sinottica" & network_y == "Sinottica") | (network_x == "ISAC" & network_y == "Sinottica"),
            tag_suff = ((!is.na(strSym) & strSym > 0.999 & network_y != "Sinottica") | distance < 50 | (valid_days_inters > 160L & f0 > 0.3)),
            tag_mergeable = (valid_days_inters < 160L & climaticmaeT < 1.8) | monthlymaeT < 1
        ) |>
        group_by(key_x, key_y) |>
        mutate(
            tag_extavgs = all(tag_extavgs) & (prod(balance) < 0),
            tag_ivs = any(tag_ivs),
            tag_same_series = (tag_suff | tag_extavgs | tag_ava | tag_dist | tag_avs | tag_avi | tag_ivs) & distance < 4000 & tag_oksynop, #  & (valid_days_inters < 500 | any(monthlymaeT < 1))
            tag_same_sensor = (
                (overlap_union > 0.9 & tag_same_series)
            ),
            tag_same_station = (
                overlap_union > 0.9 & tag_same_series
            ),
        ) |>
        ungroup()
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series =
                (tag_same_series &
                    !(dataset_x == "ARPAFVG" & dataset_y == "ISAC" &
                        (
                            (sensor_key_x == 37L & sensor_key_y == 2294L) | # Musi
                                (sensor_key_x == 20L & sensor_key_y == 1395L) | # Forni di Sopra
                                (sensor_key_x == 22L & sensor_key_y == 1483L) | # Gemona
                                (sensor_key_x == 30L & sensor_key_y == 1792L) | # Lignano
                                (sensor_key_x == 53L) | # Tolmezzo
                                (sensor_key_x == 19L & sensor_key_y == 1268L) | # Fagagna
                                (sensor_key_x == 55L & sensor_key_y == 3733L) | # Trieste
                                (sensor_key_x == 35L & sensor_key_y == 2973L) # Zoncolan
                        )
                    ) &
                    !(dataset_x == "ISAC" & dataset_y == "SCIA" &
                        (
                            (sensor_key_x == 2294L & sensor_key_y == 2490L) | # Musi
                                (sensor_key_x == 3639L & sensor_key_y == 3942L) | # Tolmezzo
                                (sensor_key_x == 1395L & sensor_key_y != 62L) | # Forni di Sopra
                                (sensor_key_x == 1394L & sensor_key_y == 1442L) | # Forni di Sopra
                                (sensor_key_x == 1268L & sensor_key_y == 1464L) | # Fagagna
                                (sensor_key_x == 999L & sensor_key_y == 813L) | # Claut - Lesis
                                (sensor_key_x == 2097L & sensor_key_y == 2137L) | # Monfalcone
                                (sensor_key_x == 2973L & sensor_key_y == 2425L) # Rifugio Zoncolan
                        )
                    ) &
                    !(dataset_x == "ARPAFVG" & dataset_y == "SCIA" &
                        (
                            (sensor_key_x == 31L & sensor_key_y == 2138L) | # Monfalcone
                                (sensor_key_x == 20L & sensor_key_y == 62L) | # Forni di Sopra
                                (sensor_key_x == 20L & sensor_key_y == 1442L) | # Forni di Sopra
                                (sensor_key_x == 30L & sensor_key_y == 1872L) | # Lignano
                                (sensor_key_x == 53L) | #  Tolmezzo
                                (sensor_key_x == 55L & sensor_key_y == 4083L) # Trieste
                        )
                    ) &
                    !(dataset_x == "ISAC" & dataset_y == "ISAC" &
                        (
                            (sensor_key_x == 1394L & sensor_key_y == 1395L) # Forni di Sopra
                        )
                    ) &
                    !(dataset_x == "SCIA" & dataset_y == "SCIA" &
                        (
                            (sensor_key_x == 1442L & sensor_key_y == 1524L) # Forni di Sopra
                        )
                    ) &
                    !((dataset_y == "SCIA" & sensor_key_y == 3913L & sensor_key_x != 3575L) | (dataset_x == "ISAC" & sensor_key_x == 3575L & sensor_key_y != 3913L) | (dataset_y == "ISAC" & sensor_key_y == 3575L)) # Tarvisio Campo
                ) |
                    (
                        (dataset_x == "ISAC" & dataset_y == "SCIA" & sensor_key_x == 2510L & sensor_key_y == 2722L) |
                            (dataset_x == "ARPAFVG" & dataset_y == "ISAC" & sensor_key_x == 31L & sensor_key_y == 2096L) | # Monfalcone
                            (dataset_x == "ARPAFVG" & dataset_y == "SCIA" & sensor_key_x == 47L & sensor_key_y == 3735L) | # San Vito al Tagliamento
                            (dataset_x == "SCIA" & dataset_y == "SCIA" & (
                                (sensor_key_x == 4093L & sensor_key_y == 4095L) | # Udine Rivolto
                                    (sensor_key_x == 117L & sensor_key_y == 118L) | # Aviano (USAF)
                                    (sensor_key_x == 115L & sensor_key_y == 118L) | # Aviano (USAF)
                                    (sensor_key_x == 798L & sensor_key_y == 799L) # Cimolais
                            ))
                    )
        )
}
