library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |> mutate(
        tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
        tag_same_station = (overlap_union > 0.8 & f0 > 0.8),
        # same series
        tag_sseries_avs = (dataset_x == "ARPAPiemonte" & dataset_y == "SCIA") & (
            (valid_days_inters >= 160L & (f0 > 0.15 | strSym > 0.99)) |
                (valid_days_inters < 160L & strSym > 0.91)
        ),
        tag_sseries_avi = (dataset_x == "ARPAPiemonte" & dataset_y == "ISAC") & (
            (valid_days_inters >= 160L & (f0 > 0.1055 | strSym > 0.922)) |
                (valid_days_inters < 160L & strSym > 0.91)
        ),
        tag_sseries_ivs = dataset_x == "ISAC" & dataset_y == "SCIA" & (
            (valid_days_inters >= 160L & (f0 > 0.1245 | strSym > 0.921 | distance < 100)) |
                (valid_days_inters < 160L & (strSym > 0.99 | distance < 200))
        ),
        tag_sseries_ivi = (dataset_x == "ISAC" & dataset_y == "ISAC") & (
            (valid_days_inters >= 160L & ((f0 > 0.102 & distance < 1800) | (!is.na(strSym) & strSym > 0.99) | distance < 150))
        ),
        tag_sseries_svs = dataset_x == "SCIA" & dataset_y == "SCIA" & (
            (valid_days_inters >= 160L & (f0 > 0.106 | strSym > 0.99)) |
                (valid_days_inters < 160L & strSym > 0.95)
        ),
        tag_same_series = tag_sseries_avi | tag_sseries_avs | tag_sseries_ivs | tag_sseries_ivi | tag_sseries_svs | (!is.na(user_code_x) & !is.na(user_code_y) & (user_code_x == user_code_y)),
        tag_mergeable = TRUE
    )
}

tag_manual <- function(tagged_analysis) {
    # stop("Rifare")
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series & !(
            ((dataset_x == "ISAC" & dataset_y == "SCIA") &
                (sensor_key_x == 354L & sensor_key_y == 256L)) |
                (((dataset_x == "ISAC" & network_x == "ISAC") | (dataset_y == "ISAC" & network_y == "ISAC")) &
                    (!is.na(user_code_x) & !is.na(user_code_y) & (user_code_x != user_code_y))) |
                (dataset_x == "SCIA" & dataset_y == "SCIA" &
                    (sensor_key_x == 1517L & sensor_key_y == 3147L)
                )
        )
    )
}
