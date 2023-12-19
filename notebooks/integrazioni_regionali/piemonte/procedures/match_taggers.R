library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

tag_same_station <- function(analysis) {
    analysis |> mutate(
        same_station = !(network.y == "Sinottica") & !is.na(f0) & (
            strSym > 0.86 |
                (distance < 2000 & f0 > 0.11) |
                f0 > 0.7
        )
    )
}

tag_unusable <- function(analysis) {
    analysis |> mutate(
        unusable = (!is.na(maeT) & maeT > 0.5) | is.na(maeT)
    )
}

tag_same_station_internal <- function(analysis) {
    analysis |> mutate(
        same_station = !(network.x == "Sinottica" | network.y == "Sinottica") &
            (
                (is.na(f0) & (distance < 600 | strSym > 0.9)) |
                    (!is.na(f0) & (f0 > 0.7 | distance < 300 | strSym > 0.9))
            )
    )
}

tag_unusable_internal <- function(analysis) {
    analysis |> mutate(
        unusable = FALSE
    )
}
