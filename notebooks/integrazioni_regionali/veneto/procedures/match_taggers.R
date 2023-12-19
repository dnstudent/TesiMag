library(dplyr, warn.conflicts = FALSE)

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
tag_same_station <- function(analysis) {
    analysis |>
        mutate(
            same_station = (
                !is.na(f0) & (
                    (f0 >= 0.1 & valid_days_inters * f0 > 30) |
                        strSym > 0.82 |
                        distance < 500
                )
            ) |
                is.na(f0) & (
                    strSym > 0.78
                )
        ) |>
        group_by(station_id.x, station_id.y) |>
        mutate(same_station = all(same_station)) |>
        ungroup()
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
