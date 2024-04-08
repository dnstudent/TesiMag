library(dplyr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
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
            (dataset_x == "ARPAPiemonte" &
                (sensor_key_x == 122L & sensor_key_y %in% c(1227L, 1331L)) | # Domodossola
                (sensor_key_x == 181L & sensor_key_y == 2154L) # Malanotte
            ) |
                (
                    dataset_x == "ISAC" & dataset_y == "SCIA" &
                        ((sensor_key_x == 365L & sensor_key_y == 345L) | # Colle Barant / Bobbio Pellice
                            (sensor_key_x == 1227L) | #  Domodossola Rosmini
                            (sensor_key_x == 2093L & sensor_key_y == 2134L) # Mondovì
                        )
                ) |
                (
                    dataset_x == "ISAC" & dataset_y == "ISAC" &
                        ((!is.na(user_code_x) & !is.na(user_code_y) & (user_code_x != user_code_y)) |
                            (sensor_key_y == 1227L) # Domodossola
                        )
                ) |
                (dataset_x == "SCIA" & dataset_y == "SCIA" &
                    (sensor_key_x == 1934L & sensor_key_y == 4032L) | # Lago Vannino / Toggia
                    (sensor_key_x == 2154L & sensor_key_y == 2155L) | # Malanotte
                    (sensor_key_x == 2134L) # Mondovì Synop
                ) |
                (dataset_y == "SCIA" &
                    (sensor_key_y == 507L) # Borgomanero
                )
        ) |
            (dataset_x == "ARPAPiemonte" & dataset_y == "ISAC" & (
                FALSE
                # (sensor_key_x == 51L & sensor_key_y == 435L) # Bra
            )) |
            (dataset_x == "ARPAPiemonte" & dataset_y == "SCIA" & (
                (sensor_key_x == 180L & sensor_key_y == 1447L) # Monte Fraiteve
            ))
    )
}
