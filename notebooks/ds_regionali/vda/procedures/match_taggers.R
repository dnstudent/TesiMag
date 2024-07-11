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
                !!series_ids_are_("IT_VDA_AO_VALTOURNENCHE_PLATEAU_ROSA", "160520") # Plateau Rosa
            )) |
            (!!datasets_are("ISAC", "SCIA") & (
                !!series_ids_are("IT_VDA_AO_BRUSSON_CENTRALE_IDROELETTRICA", "15176") # Brusson
            ))) &
            !(!!one_station_is("ISAC", series_id = "VDA_AO_PRE_ST_DIDIER_DORA_BALTEA_01_000615700")) # Prè-Saint-Didier Ponte Dora Baltea
    )
}
