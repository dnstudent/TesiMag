library(dplyr, warn.conflicts = FALSE)
source("src/merging/pairing.R")

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
                !!datasets_are("ARPAUmbria", "ARPAUmbria") & !!user_codes_are_("200500", "81300")
            )
        )
}
