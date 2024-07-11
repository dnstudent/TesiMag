library(dplyr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
source("src/merging/pairing.R")

tag_same_series <- function(analysis) {
    analysis |> mutate(
        tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
        tag_same_station = (overlap_union > 0.8 & f0 > 0.8),
        # same series
        tag_sseries_avs = (dataset_x == "ARPAPiemonte" & dataset_y == "SCIA") & (
            ((valid_days_inters >= 160L & (f0 > 0.15 | strSym > 0.99)) |
                (valid_days_inters < 160L & strSym > 0.91)) |
                (str_c("01", user_code_x) == user_code_y)
        ),
        tag_sseries_avi = (dataset_x == "ARPAPiemonte" & dataset_y == "ISAC") & (
            (valid_days_inters >= 160L & (f0 > 0.1055 | (!is.na(strSym) & strSym > 0.922))) |
                (valid_days_inters < 160L & strSym > 0.91) |
                (!is.na(user_code_y) & user_code_x == user_code_y)
        ),
        tag_sseries_ivs = dataset_x == "ISAC" & dataset_y == "SCIA" & (
            (valid_days_inters >= 160L & (f0 > 0.1245 | (!is.na(strSym) & strSym > 0.921) | distance < 100)) |
                (valid_days_inters < 160L & (strSym > 0.99 | distance < 200)) |
                (!is.na(user_code_x) & str_c("01", user_code_x) == user_code_y)
        ),
        tag_sseries_ivi = (dataset_x == "ISAC" & dataset_y == "ISAC") & (
            (valid_days_inters >= 160L & ((f0 > 0.102 & distance < 1800) | (!is.na(strSym) & strSym > 0.99) | distance < 150))
        ),
        tag_sseries_svs = dataset_x == "SCIA" & dataset_y == "SCIA" & (
            (valid_days_inters >= 160L & (f0 > 0.106 | strSym > 0.99)) |
                (valid_days_inters < 160L & strSym > 0.95)
        ),
        tag_synopok = TRUE, # (network_x %in% c("ISAC", "Sinottica") & network_y %in% c("ISAC", "Sinottica")) | (!(network_x %in% c("ISAC", "Sinottica")) & !(network_y %in% c("ISAC", "Sinottica"))),
        tag_same_series = tag_synopok & (tag_sseries_avi | tag_sseries_avs | tag_sseries_ivs | tag_sseries_ivi | tag_sseries_svs | (!is.na(user_code_x) & !is.na(user_code_y) & (user_code_x == user_code_y))),
        tag_mergeable = TRUE
    )
}

tag_manual <- function(tagged_analysis) {
    # stop("Rifare")
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series & !(
            (dataset_x == "ARPAPiemonte" & (
                !!series_ids_are("PIE-003061-900", "IT_PIE_VB_DOMODOSSOLA_ROSMINI") | # Domodossola
                    !!series_ids_are("PIE-003061-900", "7842") | # Domodossola
                    !!series_ids_are("PIE-004091-900", "6740") # Monte Malanotte
            )
            ) |
                (!!datasets_are("ISAC", "SCIA") &
                    (
                        !!series_ids_are("PIE_TO_BOBBIO_PELLICE_C_BARANT_02_000089400", "7584") | # Colle Barant / Bobbio Pellice
                            (series_id_x == "IT_PIE_VB_DOMODOSSOLA_ROSMINI") | # Domodossola Rosmini
                            !!series_ids_are("IT_PIE_CN_MONDOVI_315_MG", "6739") # Mondovì
                    )
                ) |
                (!!datasets_are("ISAC", "ISAC") &
                    (
                        (!is.na(user_code_x) & !is.na(user_code_y) & (user_code_x != user_code_y)) |
                            (series_id_y == "IT_PIE_VB_DOMODOSSOLA_ROSMINI") # Domodossola Rosmini
                    )
                ) |
                (!!datasets_are("SCIA", "SCIA") &
                    (
                        !!series_ids_are_("7893", "8011") | # Lago Vannino / Toggia
                            !!series_ids_are_("6740", "7304") | # Monte Malanotte
                            (user_code_x == "161140") # Mondovì Synop
                    )
                ) |
                (dataset_y == "SCIA" &
                    (series_id_y == "7786") # Borgomanero
                )
        ) |
            (!!datasets_are("ARPAPiemonte", "ISAC") & (
                FALSE
                # (sensor_key_x == 51L & sensor_key_y == 435L) # Bra
            )) |
            (!!datasets_are("ARPAPiemonte", "SCIA") & (
                !!series_ids_are("PIE-001263-902", "6738") # Monte Fraiteve
            ))
    )
}
