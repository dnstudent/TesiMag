library(dplyr, warn.conflicts = FALSE)

tag_merge_same <- function(analysis) {
    analysis |>
        mutate(
            tag_coincident = (valid_days_inters >= 1L * 355L) & (f0 > 0.66),
            tag_statavg = valid_days_inters >= 355L & f0 > 0.5 & abs(balance) > 0.9,
            tag_homogenized = (valid_days_inters),
            tag_included = (overlap_min > 0.8) & (f0 > 0.9),
            tag_introduced_lag = valid_days_inters > 1 * 365L & overlap_min > 0.65 & monthlymaeT < 0.1 & f0 > 0.75 & abs(balance) < 0.3,
            tag_smalloverlap = 2L < valid_days_inters & valid_days_inters < 300L & f0 > 0.7 & abs(climaticdelT) < 1.5,
            tag_nooverlap = valid_days_inters <= 2L &
                (((is.na(delH) & delZm < 100) | delH < 100) & strSym > 0.8 & abs(climaticdelT) < 1) &
                distance < 100
        ) |>
        group_by(id_x, id_y) |>
        mutate(
            tag_coincident = all(tag_coincident),
            tag_included = all(tag_included),
            tag_introduced_lag = all(tag_introduced_lag),
            tag_statavg = any(tag_statavg) & prod(balance) <= 0,
            tag_nooverlap = any(tag_nooverlap),
            tag_merge_same = tag_coincident | tag_statavg #| tag_nooverlap #| tag_included #| tag_introduced_lag | tag_smalloverlap | tag_statavg
        ) |>
        ungroup()
}

tag_same_aggmean <- function(analysis) {
    analysis |> mutate()
}
