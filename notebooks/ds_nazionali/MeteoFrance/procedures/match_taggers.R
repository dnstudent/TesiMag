source("src/merging/pairing.R")

at_least_3 <- function(x) {
    expr((abs({{ x }} - round({{ x }}, 3L)) > 1e-4))
}

are_precise <- function(lon, lat) {
    expr((!!at_least_3({{ lon }}) & !!at_least_3({{ lat }})))
}

reliable_distance <- expr((!!are_precise(lon_x, lon_y) & !!are_precise(lat_x, lat_y)))


tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_good_diff = valid_days_inters > 100L & (!is.na(f0noint) & f0noint > 0.15) & distance < 2000,
            tag_proximity = distance < 100 & abs(delH) < 50,

            # Summary
            tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9 & valid_days_inters > 100L),
            tag_same_station = (overlap_min > 0.9 & f0 > 0.8 & valid_days_inters > 100L),
            tag_same_series = tag_good_diff | (tag_proximity & (valid_days_inters < 100L | tag_good_diff)),
        )
}


tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series
    )
}
