library(dplyr, warn.conflicts = FALSE)

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_same_sensor = overlap_union > 0.9 & f0 > 0.9,
            tag_same_station = overlap_union > 0.8 & f0 > 0.8,
            tag_noisac = (network_x != "ISAC" & network_y != "ISAC"),
            tag_compat_code = is.na(user_code_x) | is.na(user_code_y) | ((str_to_lower(user_code_x) |> str_squish()) == (str_to_lower(user_code_y) |> str_squish())),
            tag_samecode = !is.na(user_code_x) & !is.na(user_code_y) & ((str_to_lower(user_code_x) |> str_squish()) == (str_to_lower(user_code_y) |> str_squish())),
            tag_same_series = !(dataset_x == "TAA") & tag_compat_code & (valid_days_inters < 60 | monthlymaeT < 1.5) & (
                tag_samecode |
                    (valid_days_inters >= 160L & ((f0 > 0.101 & strSym > 0.49) | (distance < 800 & fsameint > 0.635))) | (distance < 320) | (!is.na(strSym) & strSym > 0.935)
            ),
            tag_mergeable = TRUE
        )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |>
        mutate(
            tag_same_series = (tag_same_series & !(
                (dataset_x == "ISAC" & dataset_y == "TAA" &
                    ((sensor_key_x == 603L & sensor_key_y == 117L) |
                        (sensor_key_x == 586L & sensor_key_y == 3553L) |
                        # Peio
                        (sensor_key_x == 3607L & sensor_key_y == 203L) |
                        # Tonale
                        (sensor_key_x == 2257L & sensor_key_y == 201L) |
                        # Monte bondone
                        (sensor_key_x == 3595L & sensor_key_y == 182L) |
                        # Lago pesce
                        (sensor_key_x == 1497L & sensor_key_y == 66L) |
                        # Sarentino
                        (sensor_key_x == 2903L & sensor_key_y == 40L) |
                        # Bressanone
                        (sensor_key_x != 3772 & sensor_key_y == 77L) |
                        (sensor_key_x == 3772 & sensor_key_y != 77L) |
                        # Rovereto
                        (sensor_key_x == 2748L & sensor_key_y %in% c(229L, 230L)) |
                        # Folgarida
                        (sensor_key_x == 3571L & sensor_key_y == 147L)
                    )
                ) |
                    (dataset_x == "ISAC" & dataset_y == "ISAC" &
                        ((sensor_key_x == 2462L & sensor_key_y == 3581L) |
                            (sensor_key_x == 586L & sensor_key_y == 3553L) |
                            # Mezzolombardo
                            (sensor_key_x == 1811L & sensor_key_y == 3591L) |
                            # Bressanone
                            (sensor_key_y == 3772L) |
                            # Cavalese
                            (sensor_key_x == 792L & sensor_key_y %in% c(3821L, 793L)) |
                            (sensor_key_y == 792L & sensor_key_x %in% c(3821L, 793L)) |
                            # Rovereto
                            (sensor_key_x == 2748L & sensor_key_y %in% c(3846L, 2749L)) |
                            (sensor_key_y == 2748L & sensor_key_x %in% c(3846L, 2749L))
                        )
                    )
            )) |
                (dataset_x == "ISAC" & dataset_y == "TAA" & sensor_key_x == 3603L & sensor_key_y == 195L) |
                (dataset_x == "ISAC" & dataset_y == "ISAC" & sensor_key_x == 3056L & sensor_key_y == 3801L)
        )
}
