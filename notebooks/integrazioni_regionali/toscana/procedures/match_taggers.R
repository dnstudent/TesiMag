library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station = (overlap_union > 0.8 & f0 > 0.8),
            tag_same_series = distance < 300 & !(name_x == "Vallombrosa" & name_y == "Vallombrosa"),
            tag_mergeable = TRUE,
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis
}
