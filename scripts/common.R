library(ggplot2)
library(duckplyr)

tesisave <- function(file, plot = last_plot(), width = 12, ...) {
    ggsave(file, plot, width = width, dpi = 300, unit = "cm", ...)
}

load_regional_boundaries <- function(conns) {
    region_names <- c("Valle D'aosta", "Piemonte", "Lombardia", "Trentino-Alto Adige", "Veneto", "Friuli-Venezia Giulia", "Liguria", "Emilia-Romagna", "Toscana", "Umbria", "Marche")
    sf::st_read(conns$stations, query = "SELECT * FROM regional_boundaries WHERE kind = 'district' AND country = 'Italy'", geometry_column = "geometry") |>
        filter(str_to_lower(shapeName) %in% str_to_lower(region_names))
}

load_merged_meta <- function(conns, boundaries = NULL) {
    duplicates <- vroom::vroom(fs::path("db", "conv", "merged_corrected", "merge_duplicates.csv")) |>
        filter(!is.na(duplicate_of))
    if (is.null(boundaries)) boundaries <- load_regional_boundaries(conns)

    query_checkpoint_meta("full", "merged_corrected", conns$data) |>
        anti_join(duplicates, by = c("dataset", "series_key"), copy = TRUE) |>
        rename(sensor_key = series_key) |>
        filter(keep) |>
        collect() |>
        st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE) |>
        st_filter(boundaries, .predicate = st_is_within_distance, dist = set_units(50, m)) |>
        st_drop_geometry()
}
