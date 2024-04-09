library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_sseries_ava = dataset_x == "SIRToscana" & dataset_y == "SIRToscana" & (
                distance < 500 | (strSym == 1 & valid_days_inters == 0L & distance < 1000)
            ),
            tag_sseries_svs = dataset_x == "SCIA" & dataset_y == "SCIA" & (
                (valid_days_inters >= 100L & (f0 > 0.2 & !is.na(f0noint) & f0noint > 0.2))
            ),
            tag_sseries_ivi = dataset_x == "ISAC" & dataset_y == "ISAC" & (
                ((!is.na(name_x) & !is.na(name_y) & str_c(name_x, "_CAE") == name_y) |
                    (valid_days_inters > 0L & f0 > 0.999 & (!is.na(strSym) & strSym > 0.8))
                )
            ),
            tag_sseries_sva = dataset_x == "SCIA" & dataset_y == "SIRToscana" & (
                (valid_days_inters >= 100L & f0 > 0.2 & !is.na(f0noint) & f0noint > 0.2) | distance < 500
            ),
            tag_sseries_iva = dataset_x == "ISAC" & dataset_y == "SIRToscana" & (
                (valid_days_inters >= 100L & f0 > 0.1057 & !is.na(f0noint) & f0noint > 0.09)
            ),
            tag_sseries_ivs = dataset_x == "ISAC" & dataset_y == "SCIA" & (
                (valid_days_inters >= 100L & f0 > 0.135 & !is.na(f0noint) & f0noint > 0.12)
            ),
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station = (overlap_union > 0.8 & f0 > 0.8),
            tag_same_series = tag_sseries_ava | tag_sseries_iva | tag_sseries_sva | tag_sseries_svs | tag_sseries_ivi | tag_sseries_ivs,
            tag_mergeable = TRUE,
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series = tag_same_series &
                !(
                    (dataset_x == "SCIA" & dataset_y == "SCIA" &
                        ((sensor_key_x == 1497L & sensor_key_y == 1900L) | #  Firenze Ximeniano
                            (sensor_key_x %in% c(4359L, 4360L) | sensor_key_y %in% c(4359L, 4360L)) | # Volterra
                            (sensor_key_x %in% c(4224L, 4361L) & !(sensor_key_y %in% c(4224L, 4361L))) | # Volterra
                            (sensor_key_y %in% c(4224L, 4361L) & !(sensor_key_x %in% c(4224L, 4361L))) | # Volterra
                            (sensor_key_x == 228L & sensor_key_y == 229L) | # Arezzo Synop
                            (sensor_key_x == 3106L & sensor_key_y == 3107L) # Pontremoli Annunziata
                        )
                    ) |
                        (dataset_x == "ISAC" & dataset_y == "SIRToscana" &
                            ((sensor_key_x == 1326L & sensor_key_y == 141L) | #  Firenze Ximeniano
                                (sensor_key_x == 2706L & sensor_key_y == 32L) | # Poggio Casciano / Bagno a Ripoli
                                (sensor_key_x == 826L & sensor_key_y == 17L) | # Castiglion Fibocchi / Arezzo
                                (sensor_key_x %in% c(3986L, 3988L) & !(sensor_key_y %in% c(436L, 439L))) | # Volterra
                                (!(sensor_key_x %in% c(3986L, 3988L)) & sensor_key_y %in% c(436L, 439L)) | # Volterra
                                (sensor_key_x == 3987L & sensor_key_y != 440L) | # Volterra
                                (sensor_key_x != 3987L & sensor_key_y == 440L) | # Volterra
                                (sensor_key_y %in% c(437L, 438L)) | # Volterra
                                (sensor_key_x == 3928L & sensor_key_y != 431L) | # Villafranca
                                (sensor_key_x == 2246L & sensor_key_y == 219L) | # Monte Serra
                                (sensor_key_x == 2848L & sensor_key_y == 327L) | # Prato
                                (sensor_key_x == 2846L & sensor_key_y == 325L) # Prato
                            )
                        ) |
                        (dataset_x == "SIRToscana" & dataset_y == "SIRToscana" &
                            ((sensor_key_x == 136L & sensor_key_y == 141L) | #  Firenze Ximeniano
                                (sensor_key_x == 436L & sensor_key_y != 439L) | # Volterra
                                (sensor_key_x != 436L & sensor_key_y == 439L) | # Volterra
                                (sensor_key_x %in% c(437L, 438L, 440L) | sensor_key_y %in% c(437L, 438L, 440L)) | # Volterra
                                (sensor_key_x == 221L | sensor_key_y == 221) # Monte Serra
                            )
                        ) |
                        (dataset_x == "SCIA" & dataset_y == "SIRToscana" &
                            ((sensor_key_x == 229L & sensor_key_y == 17L) | # Arezzo Synop
                                (sensor_key_x == 228L & sensor_key_y == 19L) | # Arezzo Synop
                                (sensor_key_x == 3106L & sensor_key_y == 317L) | # Pontremoli Annunziata
                                (sensor_key_x == 4360L & !(sensor_key_y %in% c(436L, 439L))) | # Volterra
                                (sensor_key_x != 4360L & sensor_key_y %in% c(436L, 439L)) | # Volterra
                                (sensor_key_x %in% c(4224L, 4361L) & sensor_key_y != 438L) | # Volterra
                                (!(sensor_key_x %in% c(4224L, 4361L)) & sensor_key_y == 438L) | # Volterra
                                (sensor_key_x == 4359L & sensor_key_y != 437L) | # Volterra
                                (sensor_key_x != 4359L & sensor_key_y == 437L) | # Volterra
                                (sensor_key_y == 440L) # Volterra
                            )
                        ) |
                        (dataset_x == "ISAC" & dataset_y == "ISAC" &
                            (
                                (sensor_key_x == 2244L & sensor_key_y == 2245L) # Monte Serra
                            )
                        )
                ) |
                (dataset_x == "SCIA" & dataset_y == "SCIA" & sensor_key_x == 4224L & sensor_key_y == 4361L) | # Volterra
                (dataset_x == "SIRToscana" & dataset_y == "SIRToscana" &
                    (
                        (sensor_key_x == 146L & sensor_key_y == 147L) | # Follonica
                            (sensor_key_x == 426L & sensor_key_y == 427L) | # Viareggio
                            (sensor_key_x == 427L & sensor_key_y == 428L) | # Viareggio
                            (sensor_key_x == 401L & sensor_key_y == 402L) # Suvereto
                    )
                ) |
                (dataset_x == "ISAC" & dataset_y == "SIRToscana" &
                    (
                        (sensor_key_x == 1681L & sensor_key_y == 177L) | # Lanciafame
                            (sensor_key_x == 2375L & sensor_key_y == 251L) # Orbetello
                    )
                ) |
                (dataset_x == "ISAC" & dataset_y == "SCIA" & sensor_key_x == 4070L & sensor_key_y == 2723L) | # Passo della Cisa
                (dataset_x == "ISAC" & dataset_y == "ISAC" & sensor_key_x == 2179L & sensor_key_y == 2303L) | # Monterotondo
                (dataset_x == "SCIA" & dataset_y == "SIRToscana" &
                    (
                        (sensor_key_x == 3140L & sensor_key_y == 327L) # Prato
                    )
                ),
        )
}
