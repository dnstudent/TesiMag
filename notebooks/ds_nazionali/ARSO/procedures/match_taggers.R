source("src/merging/pairing.R")

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_good_diff = valid_days_inters > 100L & (!is.na(f0noint) & f0noint > 0.15) & distance < 2000,
            tag_proximity = distance < 1200 & abs(delH) < 40,
            tag_climats = strSym > 0.9 & abs(delH) < 51,
            # Summary
            tag_same_sensor = valid_days_inters > 100L & (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station = valid_days_inters > 100L & (overlap_min > 0.9 & f0 > 0.8),
            tag_same_series = tag_good_diff | tag_proximity | tag_climats,
        )
}


tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series | (
            !!series_ids_are_("1824", "2249") # Bilje
        )
    )
}
