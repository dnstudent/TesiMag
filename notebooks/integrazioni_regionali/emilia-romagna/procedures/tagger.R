tag_mergeable <- function(analysis) {
    analysis |>
        mutate(
            tag_mergeable =
                ((valid_days_inters > 350L) & (f0 > 0.2 | distance < 300) & monthlymaeT <= 0.5)
        )
}
