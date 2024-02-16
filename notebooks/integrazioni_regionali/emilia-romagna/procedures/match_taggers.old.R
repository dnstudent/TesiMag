# QUESTIONI:
# - SCIA-SCIA:
#   - noto serie aggregate secondo differenti definizioni di giorno: es LACOSTAMAIATICO vs Maiatico
#   - locali, agrmet, spdstra sembrano aggregati sulla data GMT
# - Dext3r comprende tutte le serie ARPAE, però sono meno aggiornate. Vanno matchate SOLAMENTE su station_id_arpae vs series_id_dext3r

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            # Same stations
            tag_sstation_avd = (dataset_x == "ARPAE" & dataset_y == "Dext3r") &
                (station_id_x == station_id_y),
            # Same series
            tag_sseries_dvd = (dataset_x == "Dext3r" & dataset_y == "Dext3r") &
                (
                    (distance <= 200 & ((valid_days_inters < 10 & strSym > 0.6) | f0 > 0.1)) |
                        (distance > 200 & ((valid_days_inters >= 160L & f0 > 0.6) | (valid_days_inters < 160 & strSym > 0.93)))
                ),
            tag_sseries_svs = (dataset_x == "SCIA" & dataset_y == "SCIA") &
                (
                    (valid_days_inters >= 160L & f0 > 0.2) |
                        (valid_days_inters >= 30 & f0 >= 0.9 & strSym > 0.6) |
                        (valid_days_inters < 30L & (distance < 50 | strSym > 0.99))
                ),
            tag_sseries_dvs = (dataset_x == "Dext3r" & dataset_y == "SCIA") &
                (
                    (valid_days_inters >= 160L & f0 > 0.15) |
                        (valid_days_inters < 160 & strSym > 0.95 & (is.na(delH) | abs(delH) < 100 | climaticmaeT < 1))
                ),
            # Summary
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station = (overlap_min > 0.9 & f0 > 0.8) | tag_sstation_avd,
            tag_same_series = tag_sseries_dvd | tag_sseries_svs | tag_sseries_dvs
            # ARPAE -> SCIA
            # ((dataset_x == "ARPAE" & dataset_y == "SCIA") &
            #     ((valid_days_inters >= 160L & f0 > 0.3) |
            #         FALSE
            #     )
            # )
        )
}
