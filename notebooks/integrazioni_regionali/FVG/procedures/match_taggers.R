library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_extavgs = (
                dataset_x == "ARPAFVG" & valid_days_inters > 160L & abs(balance) > 0.8 & !is.na(strSym) & strSym > 0.8
            ),
            tag_dist = (
                (valid_days_inters < 160 & distance < 600) | (valid_days_inters >= 160 & monthlymaeT < 0.6 & distance < 250)
            ),
            tag_ava = (dataset_x == "ARPAFVG" & dataset_y == "ARPAFVG") & (
                distance < 50
            ),
            tag_avs = (dataset_x == "ARPAFVG" & dataset_y == "SCIA") & (
                distance <= 626L
            ),
            tag_avi = (dataset_x == "ARPAFVG" & dataset_y == "ISAC") & (
                network_y == "DPC" & distance < 1000 & (is.na(delH) | abs(delH) < 100) & ((valid_days_inters < 160 & strSym > 0.95) | monthlymaeT < 0.8)
            ),
            tag_ivs = (dataset_x == "ISAC" & dataset_y == "SCIA") & (
                (valid_days_inters >= 160L & f0 > 0.1) | (valid_days_inters < 160L & (!is.na(strSym) & strSym > 0.9))
            ),
            tag_suff = ((!is.na(strSym) & strSym > 0.999) | distance < 50),
            tag_mergeable = (valid_days_inters < 160L & climaticmaeT < 1.8) | monthlymaeT < 1
        ) |>
        group_by(key_x, key_y) |>
        mutate(
            tag_extavgs = all(tag_extavgs) & (prod(balance) < 0),
            tag_ivs = any(tag_ivs),
            tag_same_series = (tag_suff | tag_extavgs | tag_ava | tag_dist | tag_avs | tag_avi | tag_ivs) & (valid_days_inters < 500 | any(monthlymaeT < 1)),
            tag_same_sensor = (
                (overlap_union > 0.9 & tag_same_series)
            ),
            tag_same_station = (
                overlap_union > 0.9 & tag_same_series
            ),
        ) |>
        ungroup()
}
