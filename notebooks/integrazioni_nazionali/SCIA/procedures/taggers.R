library(dplyr, warn.conflicts = FALSE)

tag_analysis <- function(analysis) {
    analysis |>
        mutate(
            tag_to_merge =
                (
                    (valid_days_x > 360 & valid_days_y > 360) & (
                        (valid_days_inters >= 350 & f0 > 0.15) |
                            (valid_days_inters < 350 & ((f0 * valid_days_inters >= 27) | (strSym > 0.9 & climaticmaeT < 1))))
                )
        ) |>
        group_by(id_x, id_y) |>
        mutate(
            tag_to_merge = any(tag_to_merge)
        ) |>
        ungroup()
}
