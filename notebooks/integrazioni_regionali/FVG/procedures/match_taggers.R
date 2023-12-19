library(dplyr, warn.conflicts = FALSE)

tag_same_station <- function(analysis) {
    analysis |>
        group_by(station_id.x, station_id.y) |>
        mutate(
            same_station =
                ((network.y == "Sinottica" & (station_name.x == "" | is.na(station_name.x))) | (network.y != "Sinottica" & station_name.x != "" & !is.na(station_name.x))) &
                    ((distance < 200) |
                        (is.na(f0) & distance < 1000) |
                        (is.na(station_name.x) & (network.y == "Sinottica") & (distance < 1000)) |
                        (fsameint > 0.62 & all(maeT < 1)) |
                        (f0 > 0.09 & all(maeT < 1)) |
                        ((strSym > 0.75 & fsameint > 0.2 & abs(delT) < 0.8)) & all(maeT < 1)),
        ) |>
        replace_na(list(same_station = FALSE))
}

tag_unusable <- function(analysis) {
    analysis |> mutate(
        unusable = !is.na(monthlydelT) & (abs(monthlydelT) > 0.75)
    )
}

tag_same_station_arpa_merged <- function(analysis) {
    analysis |>
        group_by(station_id.x, station_id.y) |>
        mutate(
            same_station = !is.na(f0) & !is.na(station_name.y) & !(network.y == "Sinottica") & (
                (f0 > 0.1 & all(maeT < 1)) |
                    (fsameint > 0.571 & all(maeT < 1)) |
                    (!is.na(strSym) & strSym >= 0.78) |
                    (distance < 1000) |
                    (distance < 2200 & delH < 300)
            )
        ) |>
        ungroup()
}

tag_unusable_arpa_merged <- function(analysis) {
    analysis |> mutate(
        unusable = (
            is.na(monthlydelT) | abs(monthlydelT) >= 0.8
        )
    )
}
