library(ggplot2)
library(dplyr)
library(sf)
library(units)
library(extrafont)

source("src/database/query/data.R")

theme_defaults <- theme(
    text = element_text(family = "CM Roman"),
    plot.title = element_text(size = rel(1)),
    plot.subtitle = element_text(size = rel(0.8)),
    axis.title = element_text(size = rel(0.8)),
    axis.title.y = element_text(margin = margin(r = 7)),
    axis.text = element_text(size = rel(0.65)),
    legend.title = element_text(size = rel(0.8)),
    legend.text = element_text(size = rel(0.65)),
)
theme_quartz <- theme_defaults + theme(
    text = element_text(family = "CMU Serif")
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

maybe_st_filter <- function(meta, boundaries) {
    if (!is.null(boundaries)) {
        meta <- st_as_sf(meta, coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE) |>
            st_filter(boundaries, .predicate = st_is_within_distance, dist = set_units(10, m)) |>
            st_drop_geometry()
    }
    meta
}

load_regional_boundaries <- function(conns) {
    region_names <- c("Valle D'aosta", "Piemonte", "Lombardia", "Trentino-Alto Adige", "Veneto", "Friuli-Venezia Giulia", "Liguria", "Emilia-Romagna", "Toscana", "Umbria", "Marche")
    sf::st_read(conns$stations, query = "SELECT * FROM regional_boundaries WHERE kind = 'district' AND country = 'Italy'", geometry_column = "geometry") |>
        filter(str_to_lower(shapeName) %in% str_to_lower(region_names))
}

load_northern_area_boundaries <- function(conns) {
    regional_boundaries <- load_regional_boundaries(conns)
}

load_merged_meta <- function(conns, boundaries) {
    duplicates <- vroom::vroom(fs::path("db", "conv", "merged_corrected", "merge_duplicates.csv"), show_col_types = FALSE) |>
        filter(!is.na(duplicate_of))
    # if (is.null(boundaries)) boundaries <- load_regional_boundaries(conns)

    query_checkpoint_meta("full", "merged_corrected", conns$data) |>
        anti_join(duplicates, by = c("dataset", "series_key"), copy = TRUE) |>
        rename(sensor_key = series_key) |>
        filter(keep, district != "Corse") |>
        collect() |>
        maybe_st_filter(boundaries)
}

load_merged_data <- function(conns, meta, boundaries) {
    full_isac_meta <- query_checkpoint_meta("ISAC", "raw", conns$data) |>
        select(dataset, sensor_key, network)
    # if (is.null(boundaries)) boundaries <- load_regional_boundaries(conns)
    if (is.null(meta)) meta <- load_merged_meta(conns, boundaries)

    meta <- meta |>
        maybe_st_filter(boundaries) |>
        select(dataset, sensor_key, network) |>
        mutate(dataset = if_else(dataset == "DPC", "ISAC", dataset))
    query_checkpoint_data("full", "merged_corrected", conns$data) |>
        rename(sensor_key = series_key) |>
        left_join(full_isac_meta, by = c("from_dataset" = "dataset", "from_sensor_key" = "sensor_key")) |>
        mutate(from_dataset = if_else(from_dataset == "ISAC", network, from_dataset)) |>
        select(-network) |>
        semi_join(meta, by = c("dataset", "sensor_key"), copy = TRUE)
}

load_raw_metas <- function(ds, conns, boundaries) {
    # if (is.null(boundaries)) boundaries <- load_regional_boundaries(conns)
    query_checkpoint_meta(ds, "raw", conns$data) |>
        collect() |>
        maybe_st_filter(boundaries) |>
        mutate(dataset = if_else(dataset == "ISAC", network, dataset))
}

load_raw_datas <- function(ds, conns, meta, boundaries) {
    # if (is.null(boundaries)) boundaries <- load_regional_boundaries(conns)
    if (is.null(meta)) meta <- load_raw_meta(ds, conns, boundaries)
    meta <- meta |>
        maybe_st_filter(boundaries) |>
        select(dataset, sensor_key, network) |>
        mutate(dataset = if_else(dataset == "DPC", "ISAC", dataset))

    query_checkpoint_data(ds, "raw", conns$data) |>
        inner_join(meta, by = c("dataset", "sensor_key"), copy = TRUE) |>
        mutate(dataset = if_else(dataset == "ISAC", network, dataset)) |>
        select(-network)
}
