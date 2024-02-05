library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.8),
            tag_same_station = (str_detect(user_code_y, user_code_x) | (overlap_min > 0.8 & f0 > 0.8)),
            tag_same_series = !(network_x %in% c("Sinottica", "Mareografica")) &
                !(network_y %in% c("Sinottica", "Mareografica")) &
                !(dataset_x == "ARPAL" & dataset_y == "ARPAL" & distance > 4000) &
                !(user_code_x == "VERZI" & user_code_y == "07LOAN0") &
                (
                    (valid_days_inters >= 160L & f0 > 0.14) |
                        (valid_days_inters < 160L & (distance < 560 | strSym > 0.92) & (valid_days_inters < 10L | f0 > 0.05))
                ),
        )
}

tag_mergeable <- function(analysis) {
    analysis |>
        mutate(
            tag_mergeable = (valid_days_inters < 90L) | (!is.na(monthlymaeT) & monthlymaeT <= 0.5),
        )
}
