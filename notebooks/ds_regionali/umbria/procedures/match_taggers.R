library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |> mutate(
        tag_same_sensor = FALSE,
        tag_same_station = FALSE,
        tag_same_series = (valid_days_inters >= 100L & f0 > 0.15) | distance < 10,
        tag_mergeable = TRUE
    )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis <- tagged_analysis |>
        mutate(
            tag_same_series = tag_same_series & !(
                dataset_x == "ARPAUmbria" & dataset_y == "ARPAUmbria" & sensor_key_x == 46L & sensor_key_y == 47L
            )
        )
}
