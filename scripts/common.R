library(ggplot2)
library(duckplyr)
library(sf)
library(units)
library(extrafont)
loadfonts()

source("src/database/query/data.R")

theme_defaults <- theme(
    text = element_text(family = "CM Roman"),
    plot.title = element_text(size = rel(1)),
    plot.subtitle = element_text(size = rel(0.8)),
    axis.title = element_text(size = rel(0.8)),
    axis.title.y = element_text(margin = margin(r = 5)),
    axis.text = element_text(size = rel(0.75)),
    legend.title = element_text(size = rel(0.8)),
    legend.text = element_text(size = rel(0.75)),
)

linetype_values <- c(SCIA = "dashed", ISAC = "dotted", merged = "solid", DPC = "dotdash")
arpas_ds <- c(
    "Friuli-Venezia Giulia" = "ARPAFVG",
    "Liguria" = "ARPAL",
    "Lombardia" = "ARPALombardia",
    "Marche" = "ARPAM",
    "Piemonte" = "ARPAPiemonte",
    "Umbria" = "ARPAUmbria",
    "Veneto" = "ARPAV",
    "Emilia-Romagna" = "Dext3r",
    "Toscana" = "SIRToscana",
    "Trentino-Alto Adige" = "TAA",
    "Valle D'aosta" = NA_character_
)

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

load_merged_data <- function(conns, meta = NULL, boundaries = NULL) {
    full_isac_meta <- query_checkpoint_meta("ISAC", "raw", conns$data) |>
        select(dataset, sensor_key, network)
    if (is.null(boundaries)) boundaries <- load_regional_boundaries(conns)
    if (is.null(meta)) meta <- load_merged_meta(conns, boundaries)
    meta <- st_as_sf(meta, coords = c("lon", "lat"), crs = "EPSG:4326") |>
        st_filter(boundaries, .predicate = st_is_within_distance, dist = set_units(10, m)) |>
        st_drop_geometry() |>
        select(dataset, sensor_key, network) |>
        mutate(dataset = if_else(dataset == "DPC", "ISAC", dataset))
    query_checkpoint_data("full", "merged_corrected", conns$data) |>
        rename(sensor_key = series_key) |>
        left_join(full_isac_meta, by = c("from_dataset" = "dataset", "from_sensor_key" = "sensor_key")) |>
        mutate(from_dataset = if_else(from_dataset == "ISAC", network, from_dataset)) |>
        select(-network) |>
        semi_join(meta, by = c("dataset", "sensor_key"), copy = TRUE)
}

load_raw_metas <- function(ds, conns, boundaries = NULL) {
    if (is.null(boundaries)) boundaries <- load_regional_boundaries(conns)
    query_checkpoint_meta(ds, "raw", conns$data) |>
        collect() |>
        st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE) |>
        st_filter(regional_boundaries, .predicate = st_is_within_distance, dist = set_units(50, m)) |>
        st_drop_geometry() |>
        mutate(dataset = if_else(dataset == "ISAC", network, dataset))
}

load_raw_datas <- function(ds, conns, meta = NULL, boundaries = NULL) {
    if (is.null(boundaries)) boundaries <- load_regional_boundaries(conns)
    if (is.null(meta)) meta <- load_raw_meta(ds, conns, boundaries)
    meta <- st_as_sf(meta, coords = c("lon", "lat"), crs = "EPSG:4326") |>
        st_filter(boundaries, .predicate = st_is_within_distance, dist = set_units(50, m)) |>
        st_drop_geometry() |>
        select(dataset, sensor_key, network) |>
        mutate(dataset = if_else(dataset == "DPC", "ISAC", dataset))

    query_checkpoint_data(ds, "raw", conns$data) |>
        inner_join(meta, by = c("dataset", "sensor_key"), copy = TRUE) |>
        mutate(dataset = if_else(dataset == "ISAC", network, dataset)) |>
        select(-network)
}
