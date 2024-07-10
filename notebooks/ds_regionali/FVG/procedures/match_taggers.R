library(dplyr, warn.conflicts = FALSE)
source("src/merging/pairing.R")

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

tag_manual.old <- function(tagged_analysis) {
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
                                (sensor_key_x == 36L & sensor_key_y == 2290L) | # Muggia
                                (sensor_key_x == 35L) | # Zoncolan
                                (sensor_key_x == 11L) #  Cave del Predil
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
                                (sensor_key_x == 953L & sensor_key_y == 793L) # Chiusaforte / Raccolana
                        )
                    ) &
                    !(dataset_x == "ARPAFVG" & dataset_y == "SCIA" &
                        (
                            (sensor_key_x == 31L & sensor_key_y == 2138L) | # Monfalcone
                                (sensor_key_x == 20L & sensor_key_y == 62L) | # Forni di Sopra
                                (sensor_key_x == 20L & sensor_key_y == 1442L) | # Forni di Sopra
                                (sensor_key_x == 30L & sensor_key_y == 1872L) | # Lignano
                                (sensor_key_x == 36L & sensor_key_y == 2221L) | # Muggia
                                (sensor_key_x == 53L) | #  Tolmezzo
                                (sensor_key_x == 35L) | # Zoncolan
                                (sensor_key_x == 55L & sensor_key_y == 4083L) | # Trieste
                                (sensor_key_x == 11L) # Cave del Predil
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

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series =
                (tag_same_series &
                    !(!!datasets_are("ARPAFVG", "ISAC") &
                        (
                            !!series_ids_are("MUS", "FVG_UD_MUSI_02_000231600") | # Musi
                                !!series_ids_are("FSP", "FVG_UD_FORNI_DI_SOPRA_02_000231300") | # Forni di Sopra
                                !!series_ids_are("GEM", "FVG_UD_GEMONA_DEL_FRIULI_02_000232500") | # Gemona
                                !!series_ids_are("LIG", "FVG_UD_LIGNANO_SABBIADORO_02_000343800") | # Lignano
                                (series_id_x == "TOL") | # Tolmezzo
                                !!series_ids_are("FAG", "FVG_UD_FAGAGNA_02_000393300") | # Fagagna
                                !!series_ids_are("TRI", "FVG_TS_TRIESTE_METEO_ISTITUTO_NAUTICO_02_000401700") | # Trieste
                                !!series_ids_are("MGG", "FVG_TS_MUGGIA_02_000344200") | # Muggia
                                (series_id_x == "ZON") | # Zoncolan
                                (series_id_x == "CDP") | #  Cave del Predil
                                !!series_ids_are("PIA", "FVG_PN_PIANCAVALLO_02_000103300") # Piancavallo
                        )
                    ) &
                    !(!!datasets_are("ISAC", "SCIA") &
                        (
                            !!series_ids_are("FVG_UD_MUSI_02_000231600", "6087") | # Musi
                                !!series_ids_are("FVG_UD_TOLMEZZO_02_000149200", "8424") | # Tolmezzo
                                (series_id_x == "FVG_UD_FORNI_DI_SOPRA_02_000231300" & series_id_y != "8381") | # Forni di Sopra
                                !!series_ids_are("FVG_UD_FORNI_DI_SOPRA_01_200251411", "8380") | # Forni di Sopra
                                !!series_ids_are("FVG_UD_FAGAGNA_02_000393300", "6069") | # Fagagna
                                !!series_ids_are("FVG_PN_CLAUT_02_000232400", "8335") | # Claut - Lesis
                                !!series_ids_are("FVG_GO_MONFALCONE_PLUVIO_02_000501000", "8690") | # Monfalcone
                                !!series_ids_are("FVG_UD_CHIUSAFORTE_02_000230900", "8438") | # Chiusaforte / Raccolana
                                !!one_station_is("SCIA", series_id = "8707") # Tarvisio storica
                        )
                    ) &
                    !(!!datasets_are("ARPAFVG", "SCIA") &
                        (
                            !!series_ids_are("MNF", "8692") | # Monfalcone
                                !!series_ids_are("FSP", "8381") | # Forni di Sopra
                                !!series_ids_are("FSP", "8380") | # Forni di Sopra
                                !!series_ids_are("LIG", "8604") | # Lignano
                                !!series_ids_are("MGG", "8702") | # Muggia
                                (series_id_x == "TOL") | #  Tolmezzo
                                (series_id_x == "ZON") | # Zoncolan
                                !!series_ids_are("TRI", "6066") | # Trieste
                                (series_id_x == "CDP") | # Cave del Predil
                                !!series_ids_are("PIA", "8349") | # Piancavallo
                                !!one_station_is("SCIA", series_id = "8707") # Tarvisio storica
                        )
                    ) &
                    !(!!datasets_are_("ISAC", "ISAC") &
                        (
                            !!series_ids_are_("FVG_UD_FORNI_DI_SOPRA_01_200251411", "FVG_UD_FORNI_DI_SOPRA_02_000231300") # Forni di Sopra
                        )
                    ) &
                    !(!!datasets_are_("SCIA", "SCIA") &
                        (
                            !!series_ids_are_("8380", "6084") | # Forni di Sopra
                                (user_code_x == "160360" | user_code_y == "160360") | # Aviano (non USAF)
                                !!one_station_is("SCIA", series_id = "8707") # Tarvisio storica
                        )
                    ) &
                    !((dataset_y == "SCIA" & series_id_y == "8706" & series_id_x != "FVG_UD_TARVISIO_02_000208900") | (dataset_x == "ISAC" & series_id_x == "FVG_UD_TARVISIO_02_000208900" & series_id_y != "8706") | (dataset_y == "ISAC" & series_id_y == "FVG_UD_TARVISIO_02_000208900")) # Tarvisio Campo
                ) |
                    (
                        (!!datasets_are("ISAC", "SCIA") & !!series_ids_are("FVG_UD_PASSO_PREDIL_02_000487200", "8710")) | # Passo del Predil
                            (!!datasets_are("ARPAFVG", "ISAC") & !!series_ids_are("MNF", "FVG_GO_MONFALCONE_02_000343400")) | # Monfalcone
                            (!!datasets_are("ARPAFVG", "SCIA") & !!series_ids_are("SAN", "5483")) | # San Vito al Tagliamento
                            (!!datasets_are_("SCIA", "SCIA") & (
                                !!series_ids_are_("6702", "6700") | # Udine Rivolto
                                    !!series_ids_are_("6695", "6696") | # Aviano (USAF)
                                    !!series_ids_are_("8333", "8334") # Cimolais
                            ))
                    )
        )
}
