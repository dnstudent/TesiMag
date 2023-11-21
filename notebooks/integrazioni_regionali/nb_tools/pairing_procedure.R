library(arrow, warn.conflicts = FALSE)
library(stars, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/load/load.R")
source("src/pairing/analysis.R")
source("src/pairing/matching.R")
source("src/pairing/plots.R")
source("src/pairing/combining.R")

plot_availabilities <- function(metadata.x, data.x, metadata.y, data.y) {
    plot_state_avail(bind_rows(
        db1 = s.scia |> mutate(identifier = as.character(identifier)),
        db2 = s.dpc,
        .id = "db"
    ))
}

load_scia_metadata <- function(state, buffer_size_km = 5) {
    dem <- read_stars("temp/dem/dem30.tif")
    state_buffer <- load.italian_boundaries("state") |>
        filter(shapeName == state) |>
        st_buffer(dist = units::set_units(buffer_size_km, "km"))

    open.dataset("SCIA", "metadata") |>
        collect() |>
        st_md_to_sf() |>
        st_filter(state_buffer, .predicate = st_within) |>
        prepare_metadata(dem)
}

load_scia_data <- function(metadata) {
    open.dataset("SCIA", "data") |>
        semi_join(metadata, by = c("variable", "identifier"))
}
