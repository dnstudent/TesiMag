library(arrow, warn.conflicts = FALSE)
library(stars, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("notebooks/integrazioni_regionali/nb_tools/state_avail.R")
source("src/load/load.R")
source("src/analysis/data/clim_availability.R")
source("src/pairing/analysis.R")
source("src/pairing/matching.R")
source("src/pairing/plots.R")
source("src/pairing/combining.R")
source("src/pairing/displaying.R")
source("src/database/open.R")
source("src/database/write.R")

plot_availabilities <- function(data.x, data.y) {
    plot_state_avail(bind_rows(
        db1 = data.x |> mutate(identifier = as.character(identifier)),
        db2 = data.y |> mutate(identifier = as.character(identifier)),
        .id = "db"
    ))
}


load_scia_metadata <- function(buffer, dem = read_stars("temp/dem/dem30.tif")) {
    if (is.character(buffer)) {
        buffer <- load.italian_boundaries("state") |> filter(shapeName == buffer)
    }
    station_meta <- read_station_metadata("SCIA") |>
        collect() |>
        st_md_to_sf() |>
        st_filter(buffer, .predicate = st_within) |>
        st_drop_geometry() |>
        as_arrow_table(schema = station_schema)
    list(
        station_meta,
        read_series_metadata("SCIA") |>
            semi_join(station_meta, by = "station_id")
    )
}

load_dpc_metadata <- function(buffer, dem = read_stars("temp/dem/dem30.tif")) {
    open.dataset("BRUN", "metadata") |>
        filter(flavor == "qc_era5") |>
        collect() |>
        st_md_to_sf() |>
        st_filter(buffer, .predicate = st_within) |>
        prepare_metadata(dem)
}

load_scia_data <- function(series_metadata) {
    open_data("SCIA") |>
        semi_join(series_metadata, by = "series_id")
}


plot_clim_availability <- function(clim_availablility, metadata) {
    clim_availablility |>
        as_tibble() |>
        group_by(variable, identifier) |>
        summarise(all = all(clim_available), .groups = "drop") |>
        left_join(metadata |> group_by(identifier) |> slice_tail() |> select(identifier), by = "identifier") |>
        st_as_sf() |>
        ggplot() +
        geom_sf(aes(color = all)) +
        facet_grid(~variable)
}

n_available_series <- function(data, start_date, end_date) {
    is_climatology_computable(data |> group_by_key(), value, start_date, end_date) |>
        as_tibble() |>
        group_by(variable, identifier) |>
        summarise(all = all(clim_available), .groups = "drop") |>
        filter(all) |>
        nrow()
}
