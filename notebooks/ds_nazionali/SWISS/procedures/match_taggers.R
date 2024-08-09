source("src/merging/pairing.R")

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            # Summary
            tag_same_sensor = FALSE,
            tag_same_station = FALSE,
            tag_same_series = distance < 150,
        )
}


tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series | (
            (!!datasets_are_("ISAC", "MeteoSwiss") & (
                !!series_ids_are_("066304_MG", "0-20000-0-06792") # Samedan
            ))
        )
    )
}
