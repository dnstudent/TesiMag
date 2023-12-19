library(dplyr, warn.conflicts = FALSE)

tag_manual <- function(station_id.x, station_id.y) {
    (station_id.x == "6a4cb09a7a837097dae826a6fca2e0dc" & station_id.y == "ef3e22d9e837a89832d3b2d135877fbd") |
        (station_id.x == "28db49998f37c65937c773fa21243de1" & station_id.y == "a12cfe43acce4378dc44c7c2300e8ceb")
}

tag_same_station <- function(analysis) {
    analysis |>
        mutate(
            same_station = (
                (network.y != "Sinottica") &
                    (
                        distance < 1500 |
                            valid_days_inters > 365 &
                                (
                                    (f0 >= 0.095) |
                                        (f0 >= 0.02 & (1500 <= distance & distance < 2000) & abs(monthlydelT) < 0.5) |
                                        (
                                            (2000 <= distance & distance < 3000) &
                                                ((f0 > 0.03 & strSym > 0.8) | f0 > 0.085)
                                        ) |
                                        (maeT <= 0.48)
                                )
                    )
            ) | tag_manual(station_id.x, station_id.y)
        ) |>
        group_by(station_id.x, station_id.y) |>
        mutate(
            same_station = any(same_station)
        ) |>
        ungroup()
}

tag_unusable <- function(analysis) {
    analysis |> mutate(
        unusable = !is.na(monthlydelT) & abs(monthlydelT) >= 0.51
    )
}

tag_pairable <- function(analysis) {
    stop("Not implemented yet")
}

tag_same_station_internal <- function(analysis) {
    analysis |> mutate(
        same_station = (
            is.na(f0) & ((strSym > 0.75 & distance < 2000 & delH < 20) | distance < 500) |
                !is.na(f0) & (f0 > 0.2 | strSym > 0.6 | distance < 500 | strSym > 0.91)
        )
    )
}

tag_unusable_internal <- function(analysis) {
    analysis |> mutate(
        unusable = (
            (!is.na(monthlymaeT) & monthlymaeT > 2)
        )
    )
}
