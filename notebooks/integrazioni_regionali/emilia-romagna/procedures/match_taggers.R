library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

tag_same_station <- function(analysis) {
    analysis |> mutate(
        same_station = (network.y != "Sinottica") & (
            ((valid_days_inters > 360) & (f0 >= 0.1 | fsameint >= 0.67 | (distance < 800 & maeT < 0.9) | strSym > 0.9)) |
                ((valid_days_inters <= 360) & (distance < 200 | (strSym > 0.83 & (is.na(maeT) | (!is.na(maeT) & maeT < 1)))))
        )
    )
}

tag_unusable <- function(analysis) {
    analysis |>
        mutate(
            unusable = (!is.na(f0) & (maeT > 1)) | (is.na(f0) & FALSE)
        )
}

tag_same_station_internal <- function(analysis) {
    analysis |> mutate(
        same_station = (!xor(network.x == "Sinottica", network.y == "Sinottica")) & (
            (valid_days_inters > 360 & ((f0 > 0.12 & delH < 100) | distance < 100 | strSym > 0.9)) |
                (valid_days_inters <= 360 & (distance < 100 | strSym > 0.83))
        )
    )
}

tag_unusable_internal <- function(analysis) {
    analysis |> mutate(
        unusable = (
            (!is.na(delT) & abs(delT) > 0.5)
        )
    )
}
