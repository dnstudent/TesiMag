library(dplyr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)

source("src/merging/pairing.R")

tag_same_series <- function(analysis) {
    analysis |> mutate(
        tag_same_sensor = (overlap_union > 0.9 & f0 > 0.9),
        tag_same_station = (overlap_union > 0.8 & f0 > 0.8),
        tag_sseries_svs = !!datasets_are("SCIA", "SCIA") & (
            ((valid_days_inters >= 160L & f0 > 0.7) | distance < 100)
        ),
        tag_sseries_ivi = !!datasets_are("ISAC", "ISAC") & (
            (valid_days_inters >= 160L & f0 > 0.1) | (valid_days_inters == 0L & distance < 200)
        ),
        tag_sseries_ivs = !!datasets_are("ISAC", "SCIA") & (
            (valid_days_inters >= 160L & (f0 > 0.101 | distance < 100))
        ),
        tag_dist = distance < 368,
        tag_same_series = tag_sseries_svs | tag_sseries_ivi | tag_sseries_ivs,
        tag_mergeable = TRUE
    )
}

tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = (tag_same_series |
            (!!datasets_are("ISAC", "ISAC") & (
                !!sensor_keys_are(3831L, 4048L) # Plateu Rosa
            )) |
            (!!datasets_are("ISAC", "SCIA") & (
                !!sensor_keys_are(472L, 551L) # Brusson
            )) |
            (!!datasets_are("SCIA", "SCIA") & (
                !!sensor_keys_are(69L, 3673L) # Saint Cristophe Aereoporto
            ))) &
            !(!!one_station_is("ISAC", sensor_key = 2874L)) # Prè-Saint-Didier Ponte Dora Baltea
    )
}
