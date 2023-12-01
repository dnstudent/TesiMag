library(sf, warn.conflicts = FALSE)

prepare_metadata <- function(metadata, dem) {
    metadata |> mutate(dem = st_extract(dem, geometry) |> pull(1))
}