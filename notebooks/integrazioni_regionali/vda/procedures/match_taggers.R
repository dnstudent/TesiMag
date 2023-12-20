library(dplyr, warn.conflicts = FALSE)


tag_same_station <- function(analysis) {
    analysis |>
        mutate(
            same_station = (
                (valid_days_inters > 360) & ((!is.na(strSym) & (f0 > 0.101 | (strSym > 0.93) | distance < 400)) | (is.na(strSym) & ((f0 > 0.6) | (distance < 600))))
            )
        ) |>
        group_by(station_id.x, station_id.y) |>
        mutate(
            same_station = any(same_station)
        ) |>
        ungroup()
}

tag_unusable <- function(analysis) {
    analysis |> mutate(
        unusable = (
            !is.na(monthlydelT) & (abs(monthlydelT) > 0.7 | maeT > 1.5)
        )
    )
}

tag_same_station_internal <- function(analysis) {
    analysis |>
        mutate(
            same_station = (
                ((valid_days_inters > 360) &
                    ((!is.na(strSym) &
                        (f0 > 0.1 | distance < 400)))) |
                    (is.na(strSym) & (distance < 600))
            )
        ) |>
        group_by(station_id.x, station_id.y) |>
        mutate(
            same_station = any(same_station)
        ) |>
        ungroup()
}

tag_unusable_internal <- function(analysis) {
    analysis |> mutate(
        unusable = (
            !is.na(monthlydelT) & (abs(monthlydelT) > 0.7 | maeT > 1.5)
        )
    )
}
