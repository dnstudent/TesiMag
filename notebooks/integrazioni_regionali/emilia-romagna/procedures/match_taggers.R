# QUESTIONI:
# - SCIA-SCIA:
#   - noto serie aggregate secondo differenti definizioni di giorno: es LACOSTAMAIATICO vs Maiatico
#   - locali, agrmet, spdstra sembrano aggregati sulla data GMT
# - Dext3r comprende tutte le serie ARPAE, però sono meno aggiornate. Vanno matchate SOLAMENTE su station_id_arpae vs series_id_dext3r

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            # Same series
            tag_sseries_dvd = (dataset_x == "Dext3r" & dataset_y == "Dext3r") &
                (
                    (distance <= 200 & ((valid_days_inters < 160L & strSym > 0.6) | (valid_days_inters >= 160L & (f0 > 0.1 | (fsameint > 0.28 & monthlymaeT < 0.8))))) |
                        (distance > 200 & ((valid_days_inters >= 160L & f0 > 0.6) | (valid_days_inters < 160 & strSym > 0.93 & climaticmaeT < 1.5)))
                ),
            # Summary
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station = (overlap_min > 0.9 & f0 > 0.8),
            tag_same_series = tag_sseries_dvd,
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series &
            !(sensor_key_x == 1203L & sensor_key_y == 1348L) &
            !(sensor_key_x == 583L & sensor_key_y == 1180L) &
            !(sensor_key_x == 583L & sensor_key_y == 650L) &
            !(sensor_key_x == 975L & sensor_key_y == 1442L) &
            !(sensor_key_x == 236L & sensor_key_y == 1650L) &
            !(sensor_key_x == 1440L & sensor_key_y == 1650L)
    )
}
