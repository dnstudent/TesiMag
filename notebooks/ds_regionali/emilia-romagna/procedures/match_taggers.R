tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            # Same series
            tag_sseries_dvd = (dataset_x == "Dext3r" & dataset_y == "Dext3r") &
                (
                    (distance <= 200 & ((valid_days_inters < 160L & strSym > 0.6) | (valid_days_inters >= 160L & (f0 > 0.1 | (fsameint > 0.28 & monthlymaeT < 0.8))))) |
                        (distance > 200 & ((valid_days_inters >= 160L & f0 > 0.6) | (valid_days_inters < 160 & strSym > 0.93 & climaticmaeT < 1.5))) |
                        (distance < 150) |
                        (strSym > 0.999 & abs(delH) < 50 & distance < 1310)
                ),
            tag_sseries_dvs = (dataset_x == "Dext3r" & dataset_y == "SCIA") &
                (
                    !is.na(f0) & f0 > 0.4
                ),
            tag_sseries_dvi = (dataset_x == "Dext3r" & dataset_y == "ISAC") &
                (
                    !is.na(f0) & f0 > 0.3
                ),
            tag_sseries_ivs = (dataset_x == "ISAC" & dataset_y == "SCIA") &
                (
                    !is.na(f0) & f0 > 0.2
                ),
            tag_dist = distance < 50,
            # Summary
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station = (overlap_min > 0.9 & f0 > 0.8),
            tag_same_series = tag_sseries_dvd | tag_sseries_dvs | tag_sseries_dvi | tag_sseries_ivs | tag_dist,
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series &
            !(
                (dataset_x == "Dext3r" & dataset_y == "Dext3r" &
                    (
                        (sensor_key_x == 559L | sensor_key_y == 559L) | # Ferrara AM
                            (sensor_key_x == 130L & sensor_key_y == 134L) | # Bologna urbana / meteo
                            (sensor_key_x == 696L & sensor_key_y == 707L) | # Imola / 2
                            (sensor_key_x == 1294L & sensor_key_y == 1295L) | # Rimini
                            (sensor_key_y == 73L) # Bardi Centrale
                    )) |
                    (dataset_x == "Dext3r" & dataset_y == "ISAC" &
                        (
                            (sensor_key_x == 1060L & sensor_key_y == 2477L) # Parma Uni / Synop
                        )
                    )

            ) |
            (dataset_x == "Dext3r" & dataset_y == "Dext3r" &
                (
                    (sensor_key_x == 904L & sensor_key_y == 905L) | # Monte Cimone
                        (sensor_key_x == 790L & sensor_key_y == 807L) | # Malborghetto
                        (sensor_key_x == 1294L & sensor_key_y == 1304L) | # Rimini
                        (sensor_key_x == 466L & sensor_key_y == 468L) | # Copparo
                        (sensor_key_x == 576L & sensor_key_y == 577L) | # Finale Emilia
                        (sensor_key_x == 580L & sensor_key_y == 582L) | # Fiorenzuola
                        (sensor_key_x == 877L & sensor_key_y == 880L) # Modena
                )
            ) |
            (dataset_x == "SCIA" & dataset_y == "SCIA" &
                (
                    (sensor_key_x == 2708L & sensor_key_y == 2709L) # Parma Synop
                )
            ) |
            (dataset_x == "ISAC" & dataset_y == "SCIA" &
                (
                    (sensor_key_x == 2477L & sensor_key_y %in% c(2708L, 2709L)) # Parma Synop
                )
            )
    )
}
