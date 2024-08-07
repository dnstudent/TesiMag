source("src/merging/pairing.R")

tag_same_series <- function(analysis) {
    analysis |>
        mutate(

            # Summary
            tag_same_sensor = FALSE,
            tag_same_station = FALSE,
            tag_same_series = distance < 10,
        )
}


tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series
    )
}
