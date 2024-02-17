library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_sseries_ava = (dataset_x == "ARPALombardia" & dataset_y == "ARPALombardia") & FALSE,
            tag_sseries_avs = (
                FALSE
            ),
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station = (overlap_min > 0.9 & f0 > 0.9),
            tag_same_series = tag_sseries_avs
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis
}
