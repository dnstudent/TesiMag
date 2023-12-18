library(dplyr, warn.conflicts = FALSE)

tag_same_station <- function(analysis) {
    analysis |>
        mutate(
            same_station =
                ((network.y == "Sinottica" & station_name.x == "") | (network.y != "Sinottica" & station_name.x != "")) &
                    ((distance < 200) |
                        (is.na(f0) & distance < 1000) |
                        (is.na(station_name.x) & (network.y == "Sinottica") & (distance < 1000)) |
                        (fsameint > 0.62) |
                        (f0 > 0.09) |
                        (strSym > 0.75 & fsameint > 0.2 & abs(delT) < 0.8)),
        ) |>
        replace_na(list(same_station = FALSE))
}

tag_unusable <- function(analysis) {
    analysis |> mutate(
        unusable = !is.na(monthlydelT) & (abs(monthlydelT) > 0.75)
    )
}

tag_same_station_arpa_merged <- function(analysis) {
    analysis |> mutate(
        same_station = !is.na(f0) & !(network.y == "Sinottica") & (
            f0 > 0.1 |
                fsameint > 0.571 |
                strSym >= 0.78 |
                distance < 1000
        )
    )
}

tag_unusable_arpa_merged <- function(analysis) {
    analysis |> mutate(
        unusable = (
            is.na(monthlydelT) | abs(monthlydelT) >= 0.8
        )
    )
}
