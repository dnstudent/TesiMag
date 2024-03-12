library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |> mutate(
        tag_same_sensor = FALSE,
        tag_same_station = FALSE,
        tag_same_series = (valid_days_inters > 160L & f0 > 0.2),
        tag_mergeable = TRUE
    )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis
}
