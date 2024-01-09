library(dplyr, warn.conflicts = FALSE)

tag_analysis <- function(analysis) {
    analysis |>
        mutate(
            tag_same_station = (
                (valid_days_inters > 360 &
                    (f0 > 0.2 & ((dataset_y != "ISAC" & monthlymaeT <= 0.5) | (dataset_y == "ISAC" & monthlymaetT <= 0.8))))
            ),
            tag_mergeable = (
                (valid_days_inters > 360 &
                    (f0 > 0.1057 & monthlymaeT < 0.5))
            )
        ) |>
        group_by(station_id_x, station_id_y) |>
        mutate(
            tag_same_station = all(tag_same_station)
        ) |>
        ungroup()
}
