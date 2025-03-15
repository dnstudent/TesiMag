library(vroom)
library(dplyr)
library(arrow)
library(sf)
library(units)
library(patchwork)
library(latex2exp)

source("scripts/common.R")
source("src/database/startup.R")
source("src/database/query/data.R")
source("notebooks/corrections/manual_corrections.R")

dotenv::load_dot_env("scripts/.env")
image_dir <- fs::path(Sys.getenv("IMAGES_DIR"), "stima_normali", "correzioni")
if (!fs::dir_exists(image_dir)) {
    fs::dir_create(image_dir)
}

theme_set(theme_bw())
linetype_values <- c(SCIA = "dashed", ISAC = "dotted", merged = "solid", DPC = "dotdash")

conns <- load_dbs()
on.exit(close_dbs(conns))
sets <- c("ER", "FVG", "LOM", "MAR", "TAA2", "TOS", "UMB", "VDA", "PIE", "LIG", "VEN")
# Load the data
merged_metadata <- merged_metadata <- query_checkpoint_meta(sets, "merged", conns$data) |>
    collect() |>
    st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE) |>
    st_filter(load_regional_boundaries(conns), .predicate = st_is_within_distance, dist = set_units(50, m)) |>
    st_drop_geometry()

corrections <- fs::path_abs("./external/correzioni") |>
    load_corrections()

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

p1 <- deltas |>
    filter(manual_loc_correction, spostamento >= 10) |>
    ggplot() +
    geom_histogram(aes(spostamento)) +
    scale_x_log10() +
    labs(x = TeX("\\Delta x [m]"), y = "") +
    ylim(0, 49)

p2 <- deltas |>
    filter(manual_elev_correction, delH >= 10) |>
    ggplot() +
    geom_histogram(aes(delH)) +
    scale_x_log10() +
    labs(x = TeX("\\Delta H [m]"), y = "") +
    ylim(0, 49)

p1 + p2 + plot_layout(axes = "collect_y") + plot_annotation(title = "Anagrafiche corrette manualmente")
ggsave(fs::path(image_dir, "corrections_deltas.pdf"), width = 10, height = 3, dpi = 300)

deltas |>
    summarise(n = n(), n_discarded = n - sum(coalesce(keep, TRUE)), n_loc_corrections = sum(spostamento >= 10), n_elev_corrections = sum(delH >= 10)) |>
    knitr::kable(format = "latex", col.names = c("Totale serie", "Scartate", "Correzioni collocazione", "Correzioni quota"), caption = "", label = "tab:n-corrections") |>
    cat(file = fs::path(image_dir, "corrections_summary.tex"), sep = "\n")
