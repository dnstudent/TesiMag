library(duckplyr)
library(arrow)
library(ggplot2)
library(patchwork)
library(ggsci)
library(grid)
library(tidyr)
library(lubridate)
library(withr)
library(stringr)
library(stars)
library(sf)
library(units)
library(assertr)
library(tikzDevice)
library(extrafont)
library(kableExtra)
source("src/database/startup.R")
source("src/database/query/data.R")
source("scripts/common.R")

loadfonts()
theme_set(theme_bw() + theme_defaults)

conns <- load_dbs()
on.exit(close_dbs(conns))
regional_boundaries <- load_regional_boundaries(conns)
northern_area_boundaries <- load_northern_area_boundaries(conns)
only_ita <- TRUE
boundaries <- if (only_ita) regional_boundaries else NULL

options(tikzDefaultEngine = "xetex")
dotenv::load_dot_env("scripts/.env")
image_dir <- fs::path(Sys.getenv("IMAGES_DIR"), "creazione_dataset", "dataset")
if (!fs::dir_exists(image_dir)) {
    fs::dir_create(image_dir)
}
table_dir <- fs::path(Sys.getenv("TABLES_DIR"), "creazione_dataset", "dataset")
if (!fs::dir_exists(table_dir)) {
    fs::dir_create(table_dir)
}
pres_dir <- fs::path(Sys.getenv("PRES_DIR"))
if (!fs::dir_exists(pres_dir)) {
    fs::dir_create(pres_dir)
}


monthly_availability_by_series <- function(data, ...) {
    data |>
        filter(!is.na(value)) |>
        group_by(..., month = as.integer(month(date)), year = as.integer(year(date)), .add = TRUE) |>
        mutate(pdatediff = as.integer(date - lag(date, order_by = date)) |> coalesce(1L)) |>
        summarise(is_month_available = (n() >= 20L) & (max(pdatediff, na.rm = TRUE) <= 4L), .groups = "drop")
}

clim_availability_by_series <- function(mavail.series, ..., .from_year, .to_year, .min_years) {
    mavail.series |>
        filter(between(year, .from_year, .to_year)) |>
        group_by(..., month) |>
        summarise(is_clim_available = sum(is_month_available) >= .min_years, .groups = "drop_last") |>
        summarise(is_clim_available = all(is_clim_available), .groups = "drop")
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

load_scia_meta <- function(only_ita) {
    meta <- query_checkpoint_meta("SCIA", "raw", conns$data) |>
        collect()
    if (only_ita) {
        meta |>
            st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE) |>
            st_filter(regional_boundaries, .predicate = st_is_within_distance, dist = set_units(50, m)) |>
            st_drop_geometry()
    } else {
        meta |> filter(lat > 42.4, !(district %in% c("Lazio", "Abruzzo")))
    }
}

load_isac_dpc_meta <- function(only_ita) {
    metas <- query_checkpoint_meta("ISAC", "raw", conns$data) |>
        collect()
    if (only_ita) {
        metas <- maybe_st_filter(metas, regional_boundaries)
    } else {
        metas <- metas |> filter(lat > 42.4, !(district %in% c("Lazio", "Abruzzo")))
    }
    list(
        isac = metas |> filter(network == "ISAC"),
        dpc = metas |> filter(network == "DPC")
    )
}

# Meta originali
ometas <- query_checkpoint_meta(c("ARSO", "GeoSphereAustria", "MeteoFrance", "SwissMetNet", "WSL", "AgroMeteo", "MeteoSwiss"), "raw", conns$data) |> filter(district != "Corse")

#  Load everything
metas <- local({
    isacdpc <- load_isac_dpc_meta(only_ita)
    list(fmerged = load_merged_meta(conns, NULL), merged = load_merged_meta(conns, boundaries), scia = load_scia_meta(only_ita), isac = isacdpc$isac, dpc = isacdpc$dpc)
})

datas <- local({
    list(
        fmerged = load_merged_data(conns, metas$fmerged, NULL),
        merged = load_merged_data(conns, metas$merged, NULL), #|> semi_join(metas$merged, by = c("dataset", "sensor_key"), copy = TRUE),
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
    clim_availability_by_series(.x, dataset, sensor_key, variable, .from_year = 1991L, .to_year = 2020L, .min_years = 5L) |> assert(not_na, dataset, sensor_key, variable)
})
climav_metas <- purrr::map2(metas, clavails, ~ {
    anti_join(.x, filter(.y, !is_clim_available), by = c("dataset", "sensor_key")) |> assert(not_na, dataset, sensor_key)
})

climav_metas$scia |>
    st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE) |>
    ggplot() +
    geom_sf()

# Datasets
rmetas <- query_checkpoint_meta(c("Dext3r", "ARPAFVG", "ARPAL", "ARPALombardia", "ARPAM", "ARPAPiemonte", "ARPAUmbria", "ARPAV", "ARSO", "SIRToscana", "TAA", "WSL"), "raw", conns$data) |>
    mutate(
        display_dataset = case_when(
            dataset %in% c("ISAC", "TAA") ~ network,
            .default = dataset
        )
    )
nmetas <- query_checkpoint_meta(c("SCIA", "ISAC"), "raw", conns$data) |>
    collect() |>
    maybe_st_filter(boundaries) |>
    mutate(
        display_dataset = case_when(
            dataset == "ISAC" ~ network,
            .default = dataset
        )
    )

# Temporali
tmavails <- purrr::map(mavails, monthly_availability) |>
    bind_rows(.id = "dataset") |>
    filter(dataset != "fmerged") |>
    mutate(dataset = case_match(dataset, "scia" ~ "SCIA", "isac" ~ "ISAC", "dpc" ~ "DPC", .default = dataset))

with_seed(0L, {
    p <- ggplot(data = tmavails |> filter(variable == -1L, is_month_available), mapping = aes(date, n, linetype = dataset)) +
        geom_line() +
        scale_linetype_manual(values = linetype_values) +
        labs(color = "Mese disponibile", linetype = "Dataset", x = "Mese", y = "Numero di serie")

    p1 <- p + scale_x_date(limits = c(min(tmavails |> filter(dataset == "merged") |> pull(date)), as.Date("1991-01-01")))
    p2 <- p + scale_x_date(limits = c(as.Date("1991-01-01"), max(tmavails |> filter(dataset == "merged") |> pull(date))))

    (p1 / p2 + plot_layout(axes = "collect", guides = "collect")) + plot_annotation(title = "Disponibilità di serie nei dataset nazionali", subtitle = "Centro-nord Italia, pre e post 1991")

    ggsave(fs::path(image_dir, "monthly_availability.tex"), width = 13.5, height = 10, units = "cm", device = tikz)
})

with_seed(0L, {
    ggplot(data = tmavails |> filter(variable == 1L, is_month_available, dataset %in% c("merged", "SCIA"), between(year(date), 1991L, 2020L)), mapping = aes(date, n, linetype = dataset)) +
        geom_line() +
        scale_linetype_manual(values = linetype_values) +
        labs(y = NULL) +
        theme(axis.title = element_blank(), axis.title.x = element_blank(), legend.position = "top", legend.title = element_blank(), legend.margin = margin(b = -0.2, unit = "cm"))

    ggsave(fs::path(pres_dir, "composizione_dataset", "monthly_availabilities.tex"), width = 6, height = 3.2, units = "cm", device = tikz)
})


# Spaziali
## Integr SCIA
merged_from_scia <- metas$merged |>
    rowwise() |>
    mutate(fromSCIA = ("SCIA" %in% from_datasets), ) |>
    ungroup() |>
    assert(not_na, dataset, sensor_key, fromSCIA)

with_seed(0L, {
    p1 <- clavails$fmerged |>
        left_join(metas$fmerged |> select(dataset, sensor_key, lon, lat)) |>
        st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE) |>
        ggplot() +
        geom_sf(data = regional_boundaries, fill = NA, linewidth = .1) +
        geom_sf(aes(color = is_clim_available), size = .2) +
        labs(color = "Climatologie", x = "Latitudine [°E]", y = "Longitudine [°N]")
    p2 <- ggplot(st_as_sf(merged_from_scia, coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE)) +
        geom_sf(data = regional_boundaries, fill = NA, linewidth = .1) +
        geom_sf(aes(color = fromSCIA), size = .2) +
        labs(color = "SCIA", x = "Latitudine [°E]", y = "Longitudine [°N]")
    (p1 + p2) +
        plot_layout(axes = "collect") +
        plot_annotation(title = "Serie del dataset merged") &
        scale_x_continuous(labels = ~.) &
        scale_y_continuous(labels = ~.) &
        guides(color = guide_legend(override.aes = list(size = 2))) &
        (theme(legend.position = "bottom", ) +
            theme_quartz)
    ggsave(fs::path(image_dir, "merged", "spatial_availability.pdf"), width = 13.5, height = 7.5, units = "cm")
})

with_seed(0L, {
    mmeta <- metas$fmerged
    clino_meta <- load_clino_meta(conns$data) |>
        collect() |>
        semi_join(metas$fmerged, by = c("dataset" = "dataset", "series_key" = "sensor_key")) |>
        st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326")
    # st_filter(mmeta, .predicate = st_is_within_distance, dist = set_units(10, m))
    ggplot() +
        geom_sf(data = clino_meta, size = 0.1) +
        theme_quartz
    ggsave(fs::path(pres_dir, "merged_map.pdf"), width = 6 * 1.1, height = 4.3 * 1.1, units = "cm")
})

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

with_seed(0L, {
    bind_rows(
        merged = climav_metas$merged,
        dem = elevations_sample,
        .id = "Origine"
    ) |>
        mutate(Origine = factor(Origine, levels = c("dem", "merged", "SCIA", "ISAC", "DPC"))) |>
        filter(!is.na(elevation)) |>
        ggplot() +
        geom_histogram(aes(elevation, fill = Origine, after_stat(density)), position = "dodge", binwidth = 250) +
        labs(x = "Elevazione [m]", y = "Densità", fill = "Sorgente", title = "Distribuzione delle quote", subtitle = "Centro-nord Italia")
    # ggsave(fs::path(image_dir, "elevation_distribution.pdf"), width = 13.5, height = 6.75, units = "cm")
    ggsave(fs::path(image_dir, "elevation_distribution.tex"), width = 13.5, height = 6.75, units = "cm", device = tikz)
})

with_seed(0L, {
    bind_rows(
        dataset = climav_metas$merged,
        dem = elevations_sample,
        .id = "Origine"
    ) |>
        mutate(Origine = factor(Origine, levels = c("dem", "dataset", "SCIA", "ISAC", "DPC"))) |>
        filter(!is.na(elevation)) |>
        ggplot() +
        geom_histogram(aes(elevation, fill = Origine, after_stat(density)), position = "dodge", binwidth = 250) +
        labs(x = "Elevazione [m]", fill = "Sorgente", y = "Densità") +
        theme(legend.position = "top", legend.margin = margin(b = -0.3, unit = "cm"), legend.title = element_blank())
    ggsave(fs::path(pres_dir, "composizione_dataset", "elevation_distribution.tex"), width = 6, height = 3.1, units = "cm", device = tikz)
})

# Contributi a merged
contribs <- datas$merged |>
    filter(between(year(date), 1991L, 2020L), !(from_dataset %in% c("WSL", "ARSO"))) |>
    count(date, from_dataset, variable) |>
    collect()
ds_levels <- c("ISAC", "ARPAPiemonte", "SIRToscana", "Dext3r", "ARPAV", "ARPALombardia", "ARPAL", "TAA", "ARPAM", "ARPAUmbria", "ARPAFVG", "SCIA", "DPC") |> rev()
with_seed(0L, {
    ggplot(contribs |> filter(variable == 1L) |> mutate(from_dataset = factor(from_dataset, levels = ds_levels, ordered = TRUE))) +
        geom_area(aes(date, n, fill = from_dataset), position = "stack") +
        scale_fill_igv() +
        labs(x = "Data", y = "Numero di serie", fill = "Dataset", title = "Contributi al merging", subtitle = "Centro-nord Italia, 1991-2020") +
        theme(legend.text = element_text(size = rel(0.6)))
    ggsave(fs::path(image_dir, "merged_contributions.pdf"), width = 13.5, height = 13.5 / 1.618 + 1, units = "cm")
    ggsave(fs::path(image_dir, "merged_contributions.tex"), width = 13.5, height = 13.5 / 1.618, units = "cm", device = tikz)
})

# Miglioramenti nelle climatologie
raw_mavs <- bind_rows(merged = mavails$merged, SCIA = mavails$scia, ISAC = mavails$isac, DPC = mavails$dpc, .id = "Origine") |>
    filter(between(year, 1991L, 2020L), variable == 1L) |>
    group_by(Origine, dataset, sensor_key, month) |>
    summarise(n_mavail = sum(is_month_available), .groups = "drop_last") |>
    summarise(n_mavail = min(n_mavail), .groups = "drop")

threshs <- tibble(at_least = seq(1L, 30L, by = 1L))
with_seed(0L, {
    raw_mavs |>
        cross_join(threshs) |>
        filter(n_mavail >= at_least) |>
        count(Origine, at_least) |>
        ggplot() +
        geom_step(aes(at_least, n, linetype = Origine)) +
        scale_linetype_manual(values = linetype_values) +
        labs(x = "Anni", y = "Numero di serie", linetype = "Origine", title = "Climatologie calcolabili per requisito di anni disponibili", subtitle = "Centro-nord Italia, 1991-2020")
    # ggsave(fs::path(image_dir, "improvements.pdf"), width = 10, height = 3, dpi = 300)
    ggsave(fs::path(image_dir, "improvements.tex"), width = 13.5, height = 6, units = "cm", device = tikz)
})

with_seed(0L, {
    raw_mavs |>
        cross_join(threshs) |>
        filter(n_mavail >= at_least, Origine %in% c("merged", "SCIA")) |>
        count(Origine, at_least) |>
        ggplot() +
        geom_step(aes(at_least, n, linetype = Origine)) +
        scale_linetype_manual(values = linetype_values) +
        labs(x = "Anni", linetype = "Origine") +
        theme(legend.position = "top", legend.margin = margin(b = -0.3, unit = "cm"), axis.title.y = element_blank(), legend.title = element_blank())
    ggsave(fs::path(pres_dir, "composizione_dataset", "improvements.tex"), width = 6, height = 3.1, units = "cm", device = tikz)
})

# Densità
area <- regional_boundaries |>
    st_union() |>
    st_area() |>
    set_units(km2) |>
    as.numeric()

cldisps <- bind_rows(clavails, .id = "Origine") |>
    count(Origine, variable, is_clim_available) |>
    group_by(Origine, is_clim_available) |>
    slice_min(n, with_ties = FALSE) |>
    filter(is_clim_available) |>
    ungroup()
counts <- purrr::map(datas, ~ as.integer(count(filter(., between(year(date), 1991L, 2020L))) |> pull(n)))
counts <- tibble(n = counts, Origine = names(counts)) |> unnest_longer("n")
mdisps <- bind_rows(mavails, .id = "Origine") |>
    filter(between(year, 1991L, 2020L)) |>
    count(Origine, variable, is_month_available) |>
    group_by(Origine, is_month_available) |>
    slice_min(n, with_ties = FALSE) |>
    filter(is_month_available) |>
    ungroup()

kable_cols <- c(
    "Dataset",
    "Densità [\\unit{\\kilo\\meter^{-2}}]",
    "Serie",
    "Climatologie\\tnote{*} \\tnote{1}",
    "Numero di mesi\\tnote{*} \\tnote{2}",
    "Numero di dati\\tnote{*} \\tnote{3}"
)
#  Attivare in caso di bisogno! Sovrascrive il file data.tex ed elimina le modifiche manuali
bind_rows(metas, .id = "Origine") |>
    group_by(Origine) |>
    summarise(n = n(), Densità = n / area, .groups = "drop") |>
    left_join(cldisps |> select(Origine, Climatologie = n), by = "Origine") |>
    left_join(mdisps |> select(Origine, Mesi = n), by = "Origine") |>
    left_join(counts |> select(Origine, entries = n), by = "Origine") |>
    mutate(Origine = factor(case_match(Origine, "scia" ~ "SCIA", "isac" ~ "ISAC", "dpc" ~ "DPC", .default = Origine), levels = c("merged", "SCIA", "ISAC", "DPC"))) |>
    arrange(Origine) |>
    select(Origine, Densità, n, Climatologie, Mesi, entries) |>
    # knitr::kable(format = "latex", col.names = kable_cols, row.names = FALSE, digits = c(0L, 3L, 0L, 0L, 0L, 0L), escape = FALSE) |>
    kbl(format = "latex", col.names = kable_cols, row.names = FALSE, digits = c(0L, 3L, 0L, 0L, 0L, 0L), escape = FALSE, booktabs = TRUE) |>
    cat(file = fs::path(table_dir, "data.tex"), sep = "\n")
