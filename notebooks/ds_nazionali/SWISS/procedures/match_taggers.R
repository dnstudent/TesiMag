source("src/merging/pairing.R")

tag_same_series <- function(analysis) {
    analysis |>
        mutate(
            tag_samecode_swiss = !!datasets_are_("MeteoSwissNBCN", "SwissMetNet") & series_id_x == series_id_y,
            tag_samecode_isac = dataset_x == "ISAC" & (str_c("0-20000-0-", user_code_x) == str_c(user_code_y, "0")),
            # Summary
            tag_same_sensor = FALSE,
            tag_same_station = FALSE,
            tag_same_series = distance < 190 | tag_samecode_swiss | tag_samecode_isac,
        )
}


tag_manual <- function(tagged_analysis) {
    tagged_analysis |> mutate(
        tag_same_series = tag_same_series | (
            !!series_ids_are_("066304_MG", "SAM") # Samedan
        )
    )
}
