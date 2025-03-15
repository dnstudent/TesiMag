library(duckplyr)
library(arrow)
library(ggplot2)
library(patchwork)
library(ggsci)
library(tidyr)
library(lubridate)
library(withr)
library(stringr)
library(stars)
library(sf)
library(units)
library(assertr)
source("src/database/startup.R")
source("src/database/query/data.R")
source("scripts/common.R")

theme_set(theme_bw())
linetype_values <- c(SCIA = "dashed", ISAC = "dotted", merged = "solid", DPC = "dotdash")

conns <- load_dbs()
on.exit(close_dbs(conns))
regional_boundaries <- load_regional_boundaries(conns)

dotenv::load_dot_env("scripts/.env")
image_dir <- fs::path(Sys.getenv("IMAGES_DIR"), "stima_normali", "dataset")
if (!fs::dir_exists(image_dir)) {
    fs::dir_create(image_dir)
}

monthly_availability_by_series <- function(data, ...) {
    data |>
        filter(!is.na(value)) |>
        group_by(..., month = as.integer(month(date)), year = as.integer(year(date)), .add = TRUE) |>
        mutate(pdatediff = as.integer(date - lag(date, order_by = date)) |> coalesce(1L)) |>
        summarise(is_month_available = (n() >= 20L) & (max(pdatediff, na.rm = TRUE) <= 4L), .groups = "drop")
}

clim_availability_by_series <- function(mavail.series, ..., .from_year = -Inf, .to_year = Inf, .min_years = 5L) {
    mavail.series |>
        filter(between(year, .from_year, .to_year)) |>
        group_by(..., month) |>
        summarise(is_clim_available = sum(is_month_available) >= .min_years, .groups = "drop_last") |>
        summarise(is_clim_available = all(is_clim_available), .groups = "drop")
}

plot_spatial_climav <- function(meta, data, dataset, boundaries = regional_boundaries) {
    cavail <- monthly_availability_by_series(data |> filter(variable == 1L), dataset, sensor_key) |>
        clim_availability_by_series(from_year = 1990L, to_year = 2020L)

    nsp <- cavail |>
        left_join(meta |> select(dataset, sensor_key, lon, lat), by = c("dataset", "sensor_key")) |>
        ungroup() |>
        collect() |>
        sf::st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326") |>
        st_filter(boundaries)

    ggplot() +
        geom_sf(data = boundaries, fill = NA) +
        geom_sf(data = nsp, aes(color = is_clim_available)) +
        labs(color = "Almeno 5 anni disponibili", x = "Longitudine", y = "Latitudine")

    if (!fs::dir_exists(fs::path(image_dir, dataset))) {
        fs::dir_create(fs::path(image_dir, dataset))
    }
    ggsave(fs::path(image_dir, dataset, "spatial_availability.pdf"), width = 7, height = 7, dpi = 300)
}

monthly_availability <- function(mavails) {
    mavails |>
        count(year, month, variable, is_month_available) |>
        mutate(date = make_date(year, month, 1L), .keep = "unused")
}

clim_availability <- function(clavails) {
    clavails |>
        count(variable, is_clim_available)
}

load_scia_meta <- function() {
    query_checkpoint_meta("SCIA", "raw", conns$data) |>
        collect() |>
        st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE) |>
        st_filter(regional_boundaries, .predicate = st_is_within_distance, dist = set_units(50, m)) |>
        st_drop_geometry()
}

load_isac_dpc_meta <- function() {
    metas <- query_checkpoint_meta("ISAC", "raw", conns$data) |>
        collect() |>
        st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE) |>
        st_filter(regional_boundaries, .predicate = st_is_within_distance, dist = set_units(50, m)) |>
        st_drop_geometry()
    list(
        isac = metas |> filter(network == "ISAC"),
        dpc = metas |> filter(network == "DPC")
    )
}

load_merged_data <- function() {
    full_isac_meta <- query_checkpoint_meta("ISAC", "raw", conns$data) |>
        select(dataset, sensor_key, network)
    query_checkpoint_data("full", "merged_corrected", conns$data) |>
        rename(sensor_key = series_key) |>
        left_join(full_isac_meta, by = c("from_dataset" = "dataset", "from_sensor_key" = "sensor_key")) |>
        mutate(from_dataset = if_else(from_dataset == "ISAC", network, from_dataset)) |>
        select(-network)
}

#  Load everything
metas <- local({
    isacdpc <- load_isac_dpc_meta()
    list(merged = load_merged_meta(conns, regional_boundaries), scia = load_scia_meta(), isac = isacdpc$isac, dpc = isacdpc$dpc)
})

datas <- local({
    list(
        merged = load_merged_data() |> semi_join(metas$merged, by = c("dataset", "sensor_key"), copy = TRUE),
        scia = query_checkpoint_data("SCIA", "raw", conns$data) |> semi_join(metas$scia, by = c("dataset", "sensor_key"), copy = TRUE),
        isac = query_checkpoint_data("ISAC", "raw", conns$data) |> semi_join(metas$isac, by = c("dataset", "sensor_key"), copy = TRUE),
        dpc = query_checkpoint_data("ISAC", "raw", conns$data) |> semi_join(metas$dpc, by = c("dataset", "sensor_key"), copy = TRUE)
    )
})

mavails <- purrr::map(datas, ~ {
    monthly_availability_by_series(.x, dataset, sensor_key, variable) |>
        collect() |>
        assert(not_na, dataset, sensor_key, variable, year, month)
})
clavails <- purrr::map(mavails, ~ {
    clim_availability_by_series(.x, dataset, sensor_key, variable, .from_year = 1990L, .to_year = 2020L) |> assert(not_na, dataset, sensor_key, variable)
})
climav_metas <- purrr::map2(metas, clavails, ~ {
    anti_join(.x, filter(.y, !is_clim_available), by = c("dataset", "sensor_key")) |> assert(not_na, dataset, sensor_key)
})

# Temporali
tmavails <- purrr::map(mavails, monthly_availability)

p <- ggplot(mapping = aes(date, n, fill = NA, color = is_month_available, linetype = dataset)) +
    geom_line(data = tmavails$merged |> mutate(dataset = "merged") |> filter(variable == -1L), position = "stack") +
    geom_line(data = tmavails$scia |> mutate(dataset = "SCIA") |> filter(variable == -1L), position = "stack") +
    geom_line(data = tmavails$isac |> mutate(dataset = "ISAC") |> filter(variable == -1L), position = "stack") +
    geom_line(data = tmavails$dpc |> mutate(dataset = "DPC") |> filter(variable == -1L), position = "stack") +
    scale_linetype_manual(values = linetype_values) +
    labs(color = "Mese disponibile", linetype = "Dataset", x = "Mese", y = "Numero di serie")

p1 <- p + scale_x_date(limits = c(min(tmavails$merge |> pull(date)), as.Date("1990-01-01")))
p2 <- p + scale_x_date(limits = c(as.Date("1990-01-01"), max(tmavails$merge |> pull(date))))

(p1 + p2 + plot_layout(ncol = 1L, axes = "collect", guides = "collect")) + plot_annotation(title = "Disponibilità di serie nei dataset nazionali", subtitle = "Centro-nord Italia, pre e post 1990")

ggsave(fs::path(image_dir, "monthly_availability.pdf"), width = 10, height = 7, dpi = 300)

# Spaziali
merged_from_scia <- metas$merged |>
    rowwise() |>
    mutate(fromSCIA = ("SCIA" %in% from_datasets), ) |>
    ungroup() |>
    assert(not_na, dataset, sensor_key, fromSCIA)
ggplot(st_as_sf(merged_from_scia, coords = c("lon", "lat"), crs = "EPSG:4326")) +
    geom_sf(aes(color = fromSCIA)) +
    labs(color = "Sequenze SCIA", x = "Longitudine", y = "Latitudine")
ggsave(fs::path(image_dir, "merged_from_scia.pdf"), width = 7, height = 7, dpi = 300)

# Distribuzione in quota
dem_tiles <- fs::dir_ls(fs::path(fs::dir_ls("/Users/davidenicoli/Local_Workspace/Datasets/COPERNICUS_DEM30/", type = "directory", regexp = "Copernicus_DSM_.+"), "DEM"), glob = "*.tif")
dem <- st_mosaic(dem_tiles) |>
    read_stars(proxy = TRUE) |>
    st_crop(regional_boundaries)
elevations_sample <- with_seed(0L, {
    st_extract(dem, st_sample(regional_boundaries, 500000L)) |>
        st_drop_geometry() |>
        rename(elevation = 1L)
})

bind_rows(
    merged = climav_metas$merged,
    SCIA = climav_metas$scia,
    ISAC = climav_metas$isac,
    DPC = climav_metas$dpc,
    dem = elevations_sample,
    .id = "Origine"
) |>
    mutate(Origine = factor(Origine, levels = c("dem", "merged", "SCIA", "ISAC", "DPC"))) |>
    filter(!is.na(elevation)) |>
    ggplot() +
    geom_histogram(aes(elevation, fill = Origine, after_stat(density)), position = "dodge", binwidth = 250) +
    labs(x = "Elevazione [m]", y = "Densità", fill = "Sorgente", title = "Distribuzione delle quote")
ggsave(fs::path(image_dir, "elevation_distribution.pdf"), width = 10, height = 5, dpi = 300)


# Contributi a merged
contribs <- datas$merged |>
    filter(between(year(date), 1990L, 2020L), !(from_dataset %in% c("WSL", "ARSO"))) |>
    count(date, from_dataset, variable) |>
    collect()
ds_levels <- datas$merged |>
    count(from_dataset) |>
    collect() |>
    arrange(n) |>
    filter(!(from_dataset %in% c("ARSO", "WSL"))) |>
    pull(from_dataset)
ggplot(contribs |> filter(variable == 1L) |> mutate(from_dataset = factor(from_dataset, levels = ds_levels, ordered = TRUE))) +
    geom_area(aes(date, n, fill = from_dataset)) +
    scale_fill_igv() +
    labs(x = "Data", y = "Numero di serie", fill = "Dataset", title = "Contributi al merging")
ggsave(fs::path(image_dir, "merged_contributions.pdf"), width = 10, height = 4, dpi = 300)


# Miglioramenti nelle climatologie
raw_mavs <- bind_rows(merged = mavails$merged, SCIA = mavails$scia, ISAC = mavails$isac, DPC = mavails$dpc, .id = "Origine") |>
    filter(between(year, 1990L, 2020L), variable == 1L) |>
    group_by(Origine, dataset, sensor_key, month) |>
    summarise(n_mavail = sum(is_month_available), .groups = "drop_last") |>
    summarise(n_mavail = min(n_mavail), .groups = "drop")

threshs <- tibble(at_least = seq(1L, 30L, by = 1L))
raw_mavs |>
    cross_join(threshs) |>
    filter(n_mavail >= at_least) |>
    count(Origine, at_least) |>
    ggplot() +
    geom_step(aes(at_least, n, linetype = Origine)) +
    scale_linetype_manual(values = linetype_values) +
    labs(x = "Anni", linetype = "Origine", title = "Climatologie calcolabili per requisito di anni disponibili", subtitle = "Centro-nord Italia, 1990-2020")
ggsave(fs::path(image_dir, "improvements.pdf"), width = 10, height = 3, dpi = 300)
