library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/paths/paths.R")
path_tre <- file.path(path.ds, "ARPA", "TRENTINO")
path_bz <- file.path(path_tre, "bolzano")

load_data <- function() {
    open_dataset(file.path(path_bz, "dataset")) |>
        rename(date = DATE, value = VALUE) |>
        mutate(original_id = cast(original_id, utf8()))
}

load_meta_bz <- function() {
    stat_ids <- read_csv_arrow(file.path(path_bz, "sensors.csv")) |>
        filter(TYPE == "LT") |>
        select(SCODE)
    stations <- sf::read_sf(file.path(path_bz, "stations.geojson")) |> semi_join(stat_ids, by = "SCODE")
    stations
}
