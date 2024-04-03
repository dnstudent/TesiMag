library(dplyr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |> mutate(
        tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
        tag_same_station = (overlap_union > 0.8 & f0 > 0.8),
        tag_sseries_svs = (dataset_x == "SCIA" & dataset_y == "SCIA") & (
            ((valid_days_inters >= 160L & f0 > 0.7) | distance < 100)
        ),
        tag_sseries_ivi = (dataset_x == "ISAC" & dataset_y == "ISAC") & (
            (valid_days_inters >= 160L & f0 > 0.1) | (valid_days_inters == 0L & distance < 200)
        ),
        tag_sseries_ivs = (dataset_x == "ISAC" & dataset_y == "SCIA") & (
            (valid_days_inters >= 160L & (f0 > 0.101 | distance < 100))
        ),
        tag_dist = distance < 368,
        tag_same_series = tag_sseries_svs | tag_sseries_ivi | tag_sseries_ivs,
        tag_mergeable = TRUE
    )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series |
            (dataset_x == "ISAC" & dataset_y == "SCIA" & (
                (sensor_key_x == 472L & sensor_key_y == 551L) # Brusson
            ))
        # &
        # !(dataset_x == "SCIA" & dataset_y == "ISAC" & (
        #     (sensor_key_x == 862L & sensor_key_y == 859L) # Champorcer
        # )) &
        # !(dataset_x == "ISAC" & dataset_y == "ISAC" & (
        #     (sensor_key_x == 858L & sensor_key_y == 859L) # Champorcer
        # ))
    )
}
