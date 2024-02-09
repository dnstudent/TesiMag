# QUESTIONI:
# - SCIA-SCIA:
#   - noto serie aggregate secondo differenti definizioni di giorno: es LACOSTAMAIATICO vs Maiatico
#   - locali, agrmet, spdstra sembrano aggregati sulla data GMT

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station =
            # ARPAE-specific
                (dataset_x == "ARPAE" & dataset_y == "ARPAE") & ((overlap_union > 0.8 & f0 > 0.9) | (valid_days_inters == 0L & distance < 1)),
            tag_same_series =
            # ARPAE-specific
                ((dataset_x == "ARPAE" & dataset_y == "ARPAE") & (distance < 200) & (valid_days_inters < 10 | f0 > 0.9)) |
                    # SCIA-specific
                    ((dataset_x == "SCIA" & dataset_y == "SCIA") & (
                        (valid_days_inters >= 160L & f0 > 0.2) |
                            (valid_days_inters >= 30 & f0 >= 0.9 & strSym > 0.6) |
                            (valid_days_inters < 30L & (distance < 50 | strSym > 0.99))
                    )) |
                    # ARPAE -> SCIA
                    ((dataset_x == "ARPAE" & dataset_y == "SCIA") &
                        ((valid_days_inters >= 160L & f0 > 0.3) |
                        
                        )
                    )
        )
}
