library(vroom)
library(dplyr)
library(arrow)
library(sf)
library(units)
library(patchwork)
library(latex2exp)
library(withr)
library(tikzDevice)

source("scripts/common.R")
source("src/database/startup.R")
source("src/database/query/data.R")
source("notebooks/corrections/manual_corrections.R")

options(tikzDefaultEngine = "xetex")
dotenv::load_dot_env("scripts/.env")
image_dir <- fs::path(Sys.getenv("IMAGES_DIR"), "stima_normali", "correzioni")
if (!fs::dir_exists(image_dir)) {
    fs::dir_create(image_dir)
}

theme_set(theme_bw() + theme_defaults)

linetype_values <- c(SCIA = "dashed", ISAC = "dotted", merged = "solid", DPC = "dotdash")

conns <- load_dbs()
on.exit(close_dbs(conns))
sets <- c("ER", "FVG", "LOM", "MAR", "TAA2", "TOS", "UMB", "VDA", "PIE", "LIG", "VEN")
# Load the data
merged_metadata <- query_checkpoint_meta(sets, "merged", conns$data) |>
    collect() |>
    st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE) |>
    st_filter(load_regional_boundaries(conns), .predicate = st_is_within_distance, dist = set_units(10, m)) |>
    st_drop_geometry()
m <- merged_metadata |>
    rowwise() |>
    mutate(from_datasets = paste0(from_datasets, collapse = ";"), from_sensor_keys = paste0(from_sensor_keys, collapse = ";")) |>
    ungroup()
corrections <- fs::path_abs("./external/correzioni") |>
    load_corrections() |>
    semi_join(m, by = c("from_datasets", "from_sensor_keys"))

corrected <- prepare_corrections(corrections, merged_metadata) |> filter(manual_loc_correction | manual_elev_correction)

deltas <- merged_metadata |>
    left_join(corrected, by = c("from_sensor_keys", "from_datasets", "dataset"), relationship = "one-to-one") |>
    mutate(lon_ok = coalesce(lon_ok, lon), lat_ok = coalesce(lat_ok, lat), elevation = coalesce(elevation, elevation_glo30), ele_ok = coalesce(ele_ok, elevation)) |>
    mutate(
        spostamento = drop_units(st_distance(
            st_as_sf(pick(lon, lat), coords = c("lon", "lat"), crs = "EPSG:4326"),
            st_as_sf(pick(lon_ok, lat_ok), coords = c("lon_ok", "lat_ok"), crs = "EPSG:4326"),
            by_element = TRUE
        )),
        delH = abs(ele_ok - elevation)
    )
with_seed(0L, {
    p1 <- deltas |>
        filter(manual_loc_correction, spostamento >= 10) |>
        ggplot() +
        geom_histogram(aes(spostamento)) +
        scale_x_log10() +
        labs(x = ("$\\Delta \\textrm{x [m]}$"), y = "") +
        ylim(0, 49)

    p2 <- deltas |>
        filter(manual_elev_correction, delH >= 10) |>
        ggplot() +
        geom_histogram(aes(delH)) +
        scale_x_log10() +
        labs(x = ("$\\Delta \\textrm{H [m]}$"), y = "") +
        ylim(0, 49)

    p1 + p2 + plot_layout(axes = "collect_y") + plot_annotation(title = "Anagrafiche corrette manualmente")
    ggsave(fs::path(image_dir, "corrections_deltas.tex"), width = 13.5, height = 4, units = "cm", device = tikz)
})

deltas |>
    summarise(n = n(), n_discarded = n - sum(coalesce(keep, TRUE)), n_loc_corrections = sum(spostamento >= 10), n_elev_corrections = sum(delH >= 10)) |>
    knitr::kable(format = "markdown", col.names = c("Totale serie", "Scartate", "Correzioni collocazione", "Correzioni quota"))
# cat(file = fs::path(image_dir, "corrections_summary.tex"), sep = "\n")
