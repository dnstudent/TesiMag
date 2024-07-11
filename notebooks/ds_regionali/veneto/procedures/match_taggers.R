library(dplyr, warn.conflicts = FALSE)
source("src/merging/pairing.R")

#' Tagger which categorically discards matching series, even if they effectively represent the same station.
#'
#' @param analysis The analysis table.
#'
#' @return A table with an additional column, refuse_tag, which is TRUE if the match is considered to be a refused combination.
tag_unusable <- function(analysis) {
    # Based on manual checks
    analysis |> mutate(
        unusable = (!is.na(monthlydelT) & abs(monthlydelT) > 0.4) | delZ > 300
    )
}

#' Tagger which identifies a match as representing the same station.
#'
#' @param analysis The analysis table.
#'
#' @return A table with an additional column, same_station_tag, which is TRUE if the match is considered to represent the same station.
tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_same_sensor = FALSE,
            tag_same_station = FALSE,
            tag_mergeable = TRUE,
            tag_sseries_as =
                (((dataset_x %in% c("ARPAV", "SCIA") | network_x == "ISAC") & (dataset_y %in% c("ARPAV", "SCIA") | network_y == "ISAC"))) &
                    (
                        (
                            valid_days_inters >= 160L & f0 > 0.1 & !is.na(f0noint) & f0noint > 0.01
                        ) |
                            (valid_days_inters < 160L & (
                                distance < 200 | (!is.na(strSym) & strSym > 0.99)
                            )) |
                            distance < 500
                    ),
            tag_sseries_avd = dataset_x == "ARPAV" & network_y == "DPC" & (
                (valid_days_inters >= 100L & (!is.na(f0noint) & f0noint > 0.12 & distance < 4000)) |
                    distance < 160
            ),
            tag_sseries_dvs = network_x == "DPC" & dataset_y == "SCIA" & (
                (valid_days_inters >= 100L & (!is.na(f0noint) & f0noint > 0.0854 & distance < 3000)) |
                    distance < 300
            ),
            tag_sseries_dvd = network_x == "DPC" & network_y == "DPC" & (
                (valid_days_inters >= 100L & (!is.na(f0noint) & f0noint > 0.9)) |
                    (valid_days_inters < 100L & distance < 160)
            ),
            tag_sseries_ivi = dataset_x == "ISAC" & dataset_y == "ISAC" & (
                valid_days_inters > 100L & f0 > 0.4
            ),
            tag_same_series = tag_sseries_as | tag_sseries_avd | tag_sseries_dvs | tag_sseries_dvd | tag_sseries_ivi,
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = (
            tag_same_series & !(
                (dataset_x == "ARPAV" & network_y != "DPC" & !is.na(user_code_x) & user_code_x == "300014148") | # Fortogna (Longarone)
                    (network_x == "DPC" & dataset_y == "SCIA" & (
                        !is.na(series_id_x) & series_id_x == "VEN_VE_VENEZIA_SEDE_02_000131900" # Venezia Sede
                    ))
            )) |
            (
                (!!datasets_are("ARPAV", "ISAC") & (
                    !!series_ids_are("256", "IT_VEN_BL_ANDRAZ") | # Andraz
                        !!series_ids_are("256", "VEN_BL_ANDRAZ_CERNADOI_01_200251582") | # Andraz
                        !!series_ids_are("264", "BEL") | # Belluno Aereoporto
                        !!series_ids_are("160", "VEN_VE_TREPORTI_02_000238900") | # Venezia Treporti
                        !!series_ids_are("37", "VEN_BL_PASSO_FALZAREGO_02_000040000") # Passo Falzarego
                )) |
                    (!!datasets_are("ISAC", "SCIA") & (
                        !!series_ids_are("PAD", "6724") # Padova Synop
                    ))
            )
    )
}

tag_pairable <- function(analysis) {
    analysis |> mutate(
        pairable = (
            (distance < 5000 & ((!is.na(delH) & delH < 200) | (delZ < 200))) &
                (abs(monthlydelT) < 0.2) &
                valid_days_inters > 400
        )
    )
}

tag_same_station_internal <- function(analysis) {
    analysis |> mutate(
        same_station = (
            (is.na(f0) & distance < 200) |
                (!is.na(f0) & (f0 > 0.1 | distance < 800 | fsameint > 0.45))
        )
    )
}

tag_unusable_internal <- function(analysis) {
    analysis |> mutate(
        unusable = (
            !is.na(monthlydelT) & abs(monthlydelT) > 1
        )
    )
}
