library(dplyr, warn.conflicts = FALSE)


tag_same_station <- function(analysis) {
    analysis |> mutate(
        same_station = (
            (valid_days_inters > 360) & ((!is.na(strSym) & (f0 > 0.101 | (strSym > 0.93) | distance < 400)) | (is.na(strSym) & (f0 > 0.6)))
        )
    )
}

tag_unusable <- function(analysis) {
    analysis |> mutate(
        unusable = (
            !is.na(delT) & delT > 0.7
        )
    )
}
