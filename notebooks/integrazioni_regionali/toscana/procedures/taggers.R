library(dplyr, warn.conflicts = FALSE)

tag_analysis <- function(analysis) {
    analysis |>
        mutate(
            tag_same_station = (
                (valid_days_inters > 360 &
                    ((dataset_y != "ISAC" & dataset_x != "ISAC") &
                        ((f0 > 0.2 & monthlymaeT <= 0.5) | fsameint > 0.66 | strSym >= 0.9)
                    ) |
                    ((dataset_x == "ISAC" | dataset_y == "ISAC") &
                        ((f0 > 0.15) | (strSym > 0.9 & abs(delZm) < 100 & monthlymaeT < 1))
                    )
                ) |
                    (valid_days_inters <= 360 &
                        f0 > 0.97 |
                        ((dataset_y != "ISAC" & dataset_x != "ISAC") &
                            distance < 200 | strSym >= 0.93
                        ) |
                        ((dataset_x == "ISAC" | dataset_y == "ISAC") &


                        )
                    )
            ),
            tag_mergeable = (
                (valid_days_inters > 360 &
                    ((dataset_y != "ISAC" & dataset_x != "ISAC") &
                        ((f0 > 0.2 | fsameint > 0.66 | strSym >= 0.9) & monthlymaeT <= 0.5)
                    ) |
                    ((dataset_x == "ISAC" | dataset_y == "ISAC") &
                        ((f0 > 0.15) | (strSym > 0.9 & abs(delZm) < 100 & monthlymaeT <= 1))
                    )
                ) |
                    (valid_days_inters <= 360 &
                        ((dataset_y != "ISAC" & dataset_x != "ISAC") &
                            distance < 200 | strSym >= 0.93
                        ) & climaticmaeT < 1.5
                    )
            ),
        ) |>
        group_by(id_x, id_y) |>
        mutate(
            tag_same_station = all(tag_same_station)
        ) |>
        ungroup()
}
