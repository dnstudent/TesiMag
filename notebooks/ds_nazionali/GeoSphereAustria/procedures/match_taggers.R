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
            tag_good_diff = valid_days_inters > 100L & (!is.na(f0noint) & f0noint > 0.07),
            tag_proximity = distance < 100 & abs(delH) < 50,
            tag_diffds = dataset_x != dataset_y,
            tag_gvg = !!datasets_are("GeoSphereAustria", "GeoSphereAustria") & (series_id_x == series_id_y),
            tag_ivi = !!datasets_are("ISAC", "ISAC") & FALSE,
            tag_gvi = !!datasets_are_("GeoSphereAustria", "ISAC") & (tag_good_diff | distance < 10 | strSym > 0.99),
            # Summary
            tag_same_sensor = tag_diffds & (overlap_union > 0.9 & f0 > 0.9),
            tag_same_station = tag_diffds & (overlap_min > 0.9 & f0 > 0.8),
            tag_same_series = tag_gvg | tag_ivi | tag_gvi,
        )
}


tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = (tag_same_series & !(
            (!!datasets_are_("GeoSphereAustria", "ISAC") & (
                !!series_ids_are_("98", "AT_MARIAPFARR_GSOD_113480_MG") | # Tamsweg / Mariapfarr
                    !!series_ids_are_("175", "AT_INNSBRUCK_AU_LOWI_GSOD_111200_MG") | # Innsbruck
                    !!series_ids_are_("9011", "AT_KUFSTEIN_GSOD_111300_MG") | # Oberndorf / Kufstein
                    !!series_ids_are_("15412", "AT_SONNBLICK_GSOD_111460") # Sonnblick
            )) |
                (!!datasets_are("GeoSphereAustria", "GeoSphereAustria") & (
                    !!station_ids_are("12300", "12302") | # Saalbach
                        !!station_ids_are("12301", "12302") | # Saalbach
                        !!station_ids_are_("14512", "14513") | # Imst
                        !!station_ids_are_("12615", "12616") | # Radstadt
                        !!station_ids_are_("12610", "12616") | # Radstadt
                        !!station_ids_are_("15710", "15712") | # Tamsweg
                        !!station_ids_are_("15711", "15712") # Tamsweg
                ))
        )) |
            (
                (!!datasets_are("GeoSphereAustria", "GeoSphereAustria") & (
                    !!series_ids_are_("14311", "14310") | # Langen am Arlberg
                        !!series_ids_are_("15505", "15506") | # Bad Hofgastein
                        !!series_ids_are_("20021", "20020") # Villacher Alpe
                ))
            )
    )
}
