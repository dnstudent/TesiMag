st_md_to_sf <- function(metadata, remove = FALSE) {
    metadata |> sf::st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = remove)
}
