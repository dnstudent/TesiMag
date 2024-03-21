library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_oksynop = ((network_x %in% c("ISAC", "Sinottica") & network_y == "Sinottica") | network_y != "Sinottica"),
            tag_sseries_avs = (dataset_x == "ARPALombardia" & dataset_y == "SCIA") & (
                (network_y == "Regionale ARPA Lombardia" & (user_code_x == user_code_y)) |
                    (network_y == "Idrografica" &
                        (distance < 1410 |
                            (abs(delH) < 300 & strSym > 0.91)
                        )
                    )
                # (distance < 350 & (is.na(delH) | abs(delH < 150)))
            ),
            tag_sseries_svs = (dataset_x == "SCIA" & dataset_y == "SCIA") & (
                (valid_days_inters >= 100L &
                    (f0 > 0.9 & !is.na(f0noint) & f0noint > 0)
                ) |
                    distance < 1000
            ),
            tag_sseries_avi = (dataset_x == "ARPALombardia" & dataset_y == "ISAC") & (
                (valid_days_inters >= 100L & f0 > 0.21) | distance < 20
            ),
            tag_sseries_ivs = (dataset_x == "ISAC" & dataset_y == "SCIA") & tag_oksynop & (
                
            ),
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9) & !(dataset_x == "ARPALombardia" & dataset_y == "ARPALombardia"),
            tag_same_station = (overlap_min > 0.9 & f0 > 0.9) & !(dataset_x == "ARPALombardia" & dataset_y == "ARPALombardia"),
            tag_same_series = tag_sseries_avs | tag_sseries_svs
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series = tag_same_series &
                !(
                    (dataset_x == "ARPALombardia" & dataset_y == "SCIA") & (
                        (sensor_key_x == 193L & sensor_key_y == 1416L) |
                            (sensor_key_x == 173L & sensor_key_y == 995L) | # Crema
                            (sensor_key_x == 27L & sensor_key_y == 947L) | # Como
                            (sensor_key_x == 123L & sensor_key_y == 1053L) | # Premana
                            (sensor_key_x == 129L & sensor_key_y == 2485L) | # Premana
                            (sensor_key_x == 244L & sensor_key_y == 3019L) | # Sondrio
                            (sensor_key_x == 252L & sensor_key_y == 356L) | # BG via Maffei / san Fermo
                            (sensor_key_x == 148L & sensor_key_y == 902L) | # Clusone
                            (sensor_key_x == 31L & sensor_key_y == 1573L) | # Livigno Passo
                            (sensor_key_x == 86L & sensor_key_y == 730L) # Cantù
                    ) |
                        ((dataset_x == "SCIA" & dataset_y == "SCIA") & (
                            (sensor_key_x == 1071L & sensor_key_y == 3021L) | # Sonico / Edolo
                                (sensor_key_x == 1859L & sensor_key_y == 3305L) | # Pianura Secchia
                                (sensor_key_x == 2341L & sensor_key_y == 2342L) | # Pavia
                                (sensor_key_x == 156L & sensor_key_y == 157L) # Aprica
                        )) |
                        (dataset_x == "ARPALombardia" & dataset_y == "ISAC" &
                            (sensor_key_x == 74L & sensor_key_y == 1820L) | #  Milano Brera
                            (sensor_key_x == 232L & sensor_key_y == 2494L) | # Porlezza
                            (sensor_key_x == 172L & sensor_key_y == 2495L) # Porlezza
                        )
                )
        )
}
