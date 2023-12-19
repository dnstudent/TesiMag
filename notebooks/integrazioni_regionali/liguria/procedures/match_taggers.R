library(dplyr, warn.conflicts = FALSE)

tag_same_station_internal <- function(analysis) {
    analysis |> mutate(
        same_station = (
            distance < 600
        )
    )
}

tag_unusable_internal <- function(analysis) {
    analysis |> mutate(
        unusable = FALSE
    )
}

tag_same_station_vsscia <- function(analysis) {
    analysis |> mutate(
        same_station = (!is.na(f0) & (f0 > 0.1 | fsameint > 0.9)) | (is.na(f0) & distance < 600 & delH < 100)
    )
}

tag_unusable_vsscia <- function(analysis) {
    analysis |> mutate(
        unusable = (!is.na(monthlydelT) & abs(monthlydelT) > 0.7) | (is.na(monthlydelT) & delH > 100)
    )
}
