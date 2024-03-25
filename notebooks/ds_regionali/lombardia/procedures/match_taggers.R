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
                (network_y == "DPC" & distance < 100) | ((valid_days_inters > 100L & f0 > 0.21) | distance < 20)
            ),
            tag_sseries_ivi = (dataset_x == "ISAC" & dataset_y == "ISAC") & (
                distance < 850
            ),
            tag_sseries_ivs = (dataset_x == "ISAC" & dataset_y == "SCIA") & (
                (
                    network_x == "ISAC" &
                        (
                            (!is.na(user_code_x) & (str_pad(user_code_x, 5L, side = "left", pad = "0") == user_code_y)) |
                                (valid_days_inters > 10000L & f0 > 0.13) |
                                (valid_days_inters > 10000L & f0 <= 0.13 & distance < 4000)
                        )
                ) |
                    (
                        network_x == "DPC" &
                            (
                                (valid_days_inters > 100L & f0 > 0.25) |
                                    distance < 200
                            )
                    )
            ),
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9) & !(dataset_x == "ARPALombardia" & dataset_y == "ARPALombardia"),
            tag_same_station = (overlap_min > 0.9 & f0 > 0.9) & !(dataset_x == "ARPALombardia" & dataset_y == "ARPALombardia"),
            tag_same_series = tag_sseries_avs | tag_sseries_svs | tag_sseries_ivs | tag_sseries_avi | tag_sseries_ivi
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series = tag_same_series &
                !(
                    (dataset_x == "ARPALombardia" & dataset_y == "SCIA") & (
                        (sensor_key_x == 107L & sensor_key_y == 1801L) | # Ispra
                            (sensor_key_x == 79L & sensor_key_y == 1214L) | # Crema
                            (sensor_key_x == 72L & sensor_key_y == 1165L) | # Como
                            (sensor_key_x == 187L & sensor_key_y == 1320L) | # Premana
                            (sensor_key_x == 186L & sensor_key_y == 3144L) | # Premana
                            (sensor_key_x == 216L & sensor_key_y == 3862L) | # Sondrio
                            (sensor_key_x == 16L & sensor_key_y == 473L) | # BG via Maffei / san Fermo
                            (sensor_key_x == 67L & sensor_key_y == 1119L) | # Clusone
                            (sensor_key_x == 122L & sensor_key_y == 1992L) | # Livigno Passo
                            (sensor_key_x == 170L & sensor_key_y == 2986L) | # Pavia
                            (sensor_key_x == 38L & sensor_key_y == 938L) | # Cantù
                            (sensor_key_x == 88L & sensor_key_y == 1348L) | # Edolo
                            (sensor_key_x == 136L & sensor_key_y == 2269L) | # Mantova
                            (sensor_key_x == 255L & sensor_key_y == 4353L) | # Voghera
                            (sensor_key_x == 255L & sensor_key_y == 3879L) # Voghera

                    ) |
                        (dataset_x == "SCIA" & dataset_y == "SCIA" &
                            (
                                (sensor_key_x == 1348L & sensor_key_y == 3863L) | # Sonico / Edolo
                                    (sensor_key_x == 2359L & sensor_key_y == 4245L) | # Pianura Secchia
                                    (sensor_key_x == 212L & sensor_key_y == 211L) | # Aprica
                                    (sensor_key_x == 136L & sensor_key_y == 2269L) | # Mantova
                                    (sensor_key_x == 135L & sensor_key_y == 2270L) | # Mantova Lunetta / Cerese
                                    (sensor_key_x == 3879L & sensor_key_y == 4352L) | # Voghera
                                    (sensor_key_x == 4284L & sensor_key_y == 4285L) # Varzi / Nivione
                            )
                        ) |
                        (dataset_x == "ARPALombardia" & dataset_y == "ISAC" &
                            (
                                (sensor_key_x == 146L & sensor_key_y == 2038L) | # Milano Brera
                                    (sensor_key_x == 144L & sensor_key_y == 2040L) | # Milano Brera / Juvara
                                    (sensor_key_x == 145L & sensor_key_y == 2035L) | # Milano Feltre / Lambrate
                                    (sensor_key_x == 182L & sensor_key_y == 2788L) | # Porlezza
                                    (sensor_key_x == 181L & sensor_key_y == 2789L) | # Porlezza
                                    (sensor_key_x == 249L & sensor_key_y == 3904L) # Vertemate
                            )
                        ) |
                        (dataset_x == "ISAC" & dataset_y == "SCIA" &
                            (
                                (sensor_key_x == 2885L & sensor_key_y == 3692L) | # Puegnago / Salò
                                    (sensor_key_x == 2036L & sensor_key_y == 2473L) | # Milano parco nord / Parco Monza
                                    (sensor_key_x == 321L & sensor_key_y == 473L) | # BG Via Goisis / Via S. Fermo
                                    (sensor_key_x == 3387L & sensor_key_y == 3862L) | # Sondrio
                                    (sensor_key_x == 1347L & sensor_key_y == 1348L) | # Edolo
                                    (sensor_key_x == 3982L & sensor_key_y == 4352L) | # Voghera
                                    (sensor_key_x == 2528L & sensor_key_y == 2985L) | # Pavia
                                    (sensor_key_x == 286L & sensor_key_y == 244L) # Barni / Asso
                            )
                        ) |
                        (dataset_x == "ISAC" & dataset_y == "ISAC" &
                            (
                                (sensor_key_x == 1924L & sensor_key_y == 1925L) # Mantova
                            )
                        )
                ) |
                (dataset_x == "ARPALombardia" & dataset_y == "ARPALombardia" &
                    (
                        (sensor_key_x == 216L & sensor_key_y == 217L) | # Sondrio
                            (sensor_key_x == 142L & sensor_key_y == 210L) | # Milano Lambrate / Segrate 2
                            (sensor_key_x == 84L & sensor_key_y == 85L) | # Darfo
                            (sensor_key_x == 6L & sensor_key_y == 7L) # Asola
                    )
                ) |
                (dataset_x == "ISAC" & dataset_y == "SCIA" &
                    (
                        (sensor_key_x == 446L & sensor_key_y == 539L) # Brescia ITAS
                    )
                ) |
                (dataset_x == "ARPALombardia" & dataset_y == "SCIA" &
                    (
                        (sensor_key_x == 100L & sensor_key_y == 898L) # Goito
                    )
                ) |
                (dataset_x == "ARPALombardia" & dataset_y == "ISAC" &
                    (
                        (sensor_key_x == 15L & sensor_key_y == 321L) | # BG via Goisis
                            (sensor_key_x == 254L & sensor_key_y == 1903L) | # Mantova Cerese
                            (sensor_key_x == 195L & sensor_key_y == 3129L) | # Samolaco
                            (sensor_key_x == 20L & sensor_key_y == 340L) # Bienno
                    )
                )
        )
}
