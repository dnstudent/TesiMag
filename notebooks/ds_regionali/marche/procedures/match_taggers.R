library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_sseries_ava = (dataset_x == "ARPAM" & dataset_y == "ARPAM") & (
                (strSym > 0.99 & (abs(delH) < 100)) | (distance < 1 & abs(delH) < 100)
            ),
            tag_sseries_avs = (dataset_x == "ARPAM" & dataset_y == "SCIA") & (
                (valid_days_inters > 100 & (f0 > 0.068 | (strSym > 0.99 & abs(delH) < 10))) |
                    (valid_days_inters <= 100 & (strSym > 0.9 & abs(delH) < 25))
            ),
            tag_sseries_avi = (dataset_x == "ARPAM" & dataset_y == "ISAC") & (
                (valid_days_inters > 100 & (f0 > 0.1 | (!is.na(strSym) & strSym > 0.99)) |
                    (valid_days_inters <= 100 & (distance < 2250 | (!is.na(strSym) & strSym > 0.99)))
                )
            ),
            tag_sseries_ivs = (dataset_x == "ISAC" & dataset_y == "SCIA") & (
                (valid_days_inters > 100 & (f0 > 0.1 | distance < 500)) |
                    (valid_days_inters <= 100 & distance < 100)
            ),
            tag_sseries_ivi = (dataset_x == "ISAC" & dataset_y == "ISAC") & (
                distance < 10
            ),
            tag_sseries_svs = (dataset_x == "SCIA" & dataset_y == "SCIA") & (
                strSym > 0.99 & abs(delH) < 50
            ),
            tag_synopok = (network_x %in% c("ISAC", "Sinottica") & network_y == "Sinottica") | (network_y != "Sinottica"),
            tag_mareok = (network_x == "Mareografica" & network_y == "Mareografica") | (network_x != "Mareografica" & network_y != "Mareografica"),
            tag_assamok = (network_x != "Regionale ASSAM Marche" & network_y != "Regionale ASSAM Marche"),
            tag_same_sensor = FALSE,
            tag_same_station = FALSE,
            tag_same_series = tag_mareok & tag_synopok & tag_assamok & (tag_sseries_ava | tag_sseries_avs | tag_sseries_avi | tag_sseries_ivs | tag_sseries_ivi),
            tag_mergeable = TRUE
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series & !(
            (dataset_x == "ARPAM" & dataset_y == "ISAC" &
                (sensor_key_x == 75L & sensor_key_y == 2804L) | # CADSEALAND / Porto Recanati
                (sensor_key_x == 116L & sensor_key_y == 2705L) # Poggio Cancelli
            )
        ) |
            (dataset_x == "ARPAM" & dataset_y == "SCIA" &
                FALSE
            )
    )
}
