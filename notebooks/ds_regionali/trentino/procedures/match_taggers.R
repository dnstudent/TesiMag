library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_same_sensor = overlap_union > 0.9 & f0 > 0.9,
            tag_same_station = overlap_union > 0.8 & f0 > 0.8,
            tag_noisac = (network_x != "ISAC" & network_y != "ISAC"),
            tag_compat_code = is.na(user_code_x) | is.na(user_code_y) | ((str_to_lower(user_code_x) |> str_squish()) == (str_to_lower(user_code_y) |> str_squish())),
            tag_same_code = !is.na(user_code_x) & !is.na(user_code_y) & ((str_to_lower(user_code_x) |> str_squish()) == (str_to_lower(user_code_y) |> str_squish())),
            tag_sseries_gen = (
                tag_compat_code & ((
                    valid_days_inters >= 100L & (!is.na(f0noint) & f0noint > 0.1026 | fsameint > 0.648 | (distance < 200 & climaticmaeT < 2.15))
                ) | (distance < 200 & climaticmaeT < 2))

            ),
            tag_sseries_svt = dataset_x == "SCIA" & dataset_y == "TAA" & (
                (valid_days_inters >= 100L & ((!is.na(f0noint) & f0noint > 0.8) | (!is.na(strSym) & strSym > 0.95 & climaticmaeT < 2.15)))
            ),
            tag_sseries_ivt = dataset_x == "ISAC" & dataset_y == "TAA" & (
                (valid_days_inters >= 100L & (fsameint > 0.7 | (distance < 400 & climaticmaeT < 1.345))) |
                    (valid_days_inters < 100 & distance < 400)
            ),
            tag_sseries_ivs = dataset_x == "ISAC" & dataset_y == "SCIA" & (
                (valid_days_inters >= 100L & f0 > 0.4) |
                    (valid_days_inters < 100 & distance < 350)
            ),
            tag_sseries_svs = dataset_x == "SCIA" & dataset_y == "SCIA" & (
                distance < 5
            ),
            # tag_same_series = dataset_x != "TAA" & tag_compat_code & (valid_days_inters < 60 | monthlymaeT < 1.5) & (
            #     tag_same_code |
            #         (valid_days_inters >= 160L & ((f0 > 0.101 & strSym > 0.49) | (distance < 800 & fsameint > 0.635))) | (distance < 320) | (!is.na(strSym) & strSym > 0.935)
            # ),
            tag_same_series = tag_same_code | tag_sseries_gen | tag_sseries_svt | tag_sseries_ivt | tag_sseries_ivs | tag_sseries_svs,
            tag_mergeable = TRUE
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series = (tag_same_series & !(
                (dataset_x == "ISAC" & dataset_y == "TAA" & (
                    (sensor_key_x == 1360L & sensor_key_y == 80L) | # Folgarida Alta / Bassa
                        (sensor_key_x %in% c(1774L, 1775L) & sensor_key_y == 108L) | # Levico
                        # (sensor_key_y == 162L) | # Peio
                        (sensor_key_x == 645L) | # Capriana
                        (sensor_key_x == 3513L) | # S. Maddalena a Casies
                        (sensor_key_y == 151L) # Passo di Costalunga
                )) |
                    (dataset_x == "ISAC" & dataset_y == "ISAC" &
                        ((sensor_key_x == 1775L & sensor_key_y == 3055L) | # Levico / Roncegno
                            (sensor_key_x == 734L & sensor_key_y == 1888L) | #  Cima / Malga Casies
                            (sensor_key_x == 1953L & sensor_key_y == 3430L) | # Gioveretto
                            (sensor_key_x == 1572L & sensor_key_y == 2759L) | # Grigno / Ponte Filippini
                            (sensor_key_x == 2028L & sensor_key_y == 2029L) | # Mezzolombardo
                            (sensor_key_x == 627L & sensor_key_y == 628L) | # Caoria / Valsorda
                            (sensor_key_x == 3513L | sensor_key_y == 3513L) # S. Maddalena a Casies
                        )) |
                    (dataset_x == "ISAC" & dataset_y == "SCIA" &
                        ((sensor_key_x == 1371L & sensor_key_y == 3339L) | # Fondo / Romeno
                            (sensor_key_x == 3712L & sensor_key_y == 4069L) | # Trento Liceo / Laste
                            (sensor_key_x == 3796L & sensor_key_y == 3873L) | # Vallarsa Parrocchia / Malga
                            (sensor_key_x %in% c(1774L, 1775L) & sensor_key_y == 1977L) | # Levico
                            (sensor_key_x == 2983L & sensor_key_y == 4217L) | # Vipiteno
                            # (sensor_key_y == 2989L) | # Peio
                            (sensor_key_x == 1953L & sensor_key_y == 1313L) | # Martello / Diga Gioveretto
                            (sensor_key_x == 3513L) | # S. Maddalena a Casies
                            (sensor_key_x == 89L & sensor_key_y == 180L) # Alpe di Siusi new / old
                        )) |
                    (dataset_x == "SCIA" & dataset_y == "TAA" &
                        ((sensor_key_x == 3040L & sensor_key_y == 41L) | # Pieve Tesino / Castello Tesino
                            (sensor_key_x == 2981L & sensor_key_y == 159L) | # Tonale Basso
                            # (sensor_key_x != 2989L & sensor_key_y == 162L) | # Peio
                            # (sensor_key_x == 2989L & sensor_key_y != 162L) | # Peio
                            (sensor_key_x == 1040L & sensor_key_y == 42L) | # Cavalese
                            (sensor_key_x == 2957L & sensor_key_y != 150L) | # Passo di Costalunga
                            (sensor_key_x != 2957L & sensor_key_y == 150L) | # Passo di Costalunga
                            (sensor_key_y == 151L) | # Passo di Costalunga
                            (sensor_key_x == 3834L & sensor_key_y == 237L) # Sesto
                        )) |
                    (dataset_x == "SCIA" & dataset_y == "SCIA" &
                        ((sensor_key_x == 1730L & sensor_key_y == 4237L) | # Grumes / Valda
                            (sensor_key_x == 3185L & sensor_key_y == 3422L) # Resia / S. Valentino
                        # (sensor_key_x == 2989L | sensor_key_y == 2989L) # Peio
                        )) |
                    (dataset_x == "TAA" & dataset_y == "TAA" &
                        ((sensor_key_x == 98L & sensor_key_y == 279L) # Grumes / Valda
                        ))
            )) |
                (dataset_x == "SCIA" & dataset_y == "TAA" & (
                    (sensor_key_x == 4277L & sensor_key_y == 285L) | # Vandoies di sopra
                        (sensor_key_x == 219L & sensor_key_y == 11L) | #  Arco
                        (sensor_key_x == 4217L & sensor_key_y == 294L) | # Vipiteno Aereoporto
                        (sensor_key_x == 2674L & sensor_key_y == 48L) # Cima Paganella
                )) |
                (dataset_x == "ISAC" & dataset_y == "TAA" & (
                    (sensor_key_x == 2469L & sensor_key_y == 95L) | #  Grigno
                        (sensor_key_x == 2028L & sensor_key_y == 123L) | # Mezzolombardo
                        (sensor_key_x == 583L & sensor_key_y == 28L) | # Campo Tures / Molini Tures
                        (sensor_key_x == 966L & sensor_key_y == 51L) | # Cimego
                        (sensor_key_x == 1876L & sensor_key_y == 112L) | # Malè
                        (sensor_key_x == 3239L & sensor_key_y == 229L) # Sarentino
                )) |
                (dataset_x == "TAA" & dataset_y == "TAA" & (
                    # sensor_key_x == 14L & sensor_key_y == 15L #  Bezzecca
                    (sensor_key_x == 52L & sensor_key_y == 53L) | # Cles
                        (sensor_key_x == 129L & sensor_key_y == 131L) #  Monte Bondone
                ))
        )
}
