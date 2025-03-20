library(dplyr)
library(ggplot2)
library(ggrepel)
library(lubridate)
library(stringr)
library(arrow)
library(OpenStreetMap)
library(sf)
library(tsibble)
library(withr)
library(tikzDevice)
source("src/database/startup.R")
source("src/database/query/data.R")
source("scripts/common.R")

theme_set(theme_bw() + theme_defaults)
options(tikzDefaultEngine = "xetex")

dotenv::load_dot_env(fs::path("scripts", ".env"))
image_dir <- fs::path(Sys.getenv("IMAGES_DIR"), "stima_normali", "raccolta_dati")
if (!fs::dir_exists(image_dir)) {
    fs::dir_create(image_dir)
}

conns <- load_dbs()
on.exit(close_dbs(conns))


reggio_series <- query_checkpoint_meta(c("Dext3r", "SCIA", "ISAC"), "raw", conns$data) |>
    filter(between(lon, 10.42, 10.80), between(lat, 44.64, 44.774642)) |>
    collect() |>
    mutate(display_dataset = if_else(dataset == "ISAC", network, dataset), sequence = str_c(display_dataset, sensor_key, sep = "/")) |>
    sf::st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326")

wdbbox <- sf::st_bbox(reggio_series |> sf::st_buffer(units::set_units(1, "km")))
sa_map <- openmap(c(wdbbox$ymax, wdbbox$xmin), c(wdbbox$ymin, wdbbox$xmax), type = "esri-topo")
stat_points <- reggio_series |>
    st_transform(crs = "EPSG:3857") |>
    mutate(coords = as.data.frame(st_coordinates(geometry))) |>
    st_drop_geometry() |>
    unnest(coords) |>
    select(dataset = display_dataset, sequence, x = X, y = Y)
with_seed(0L, {
    autoplot(sa_map) +
        geom_point(data = stat_points, aes(color = dataset, shape = dataset), size = 3) +
        geom_label_repel(data = stat_points, aes(label = sequence), force = 20, point.padding = 1, seed = 0L) +
        scale_color_brewer(palette = "Dark2") +
        theme(plot.margin = grid::unit(c(0, 0, 0, 0), "mm"))
    ggsave(fs::path(image_dir, "reggio_series_osm.pdf"), width = 10, height = 5, dpi = 300)
})



# Series
meta <- reggio_series |>
    st_drop_geometry() |>
    select(display_dataset, dataset, sensor_key, sequence, name) |>
    filter(
        (dataset == "Dext3r" & sensor_key %in% c(446L, 447L, 448L)) |
            (display_dataset == "ISAC") |
            (dataset == "SCIA" & sensor_key %in% c(3183L, 3276L, 3184L))
    )

data <- query_checkpoint_data(c("Dext3r", "ISAC", "SCIA"), "raw", conns$data) |>
    filter(variable == 1L, between(date, "1991-01-01", "2020-12-31")) |>
    inner_join(meta, by = c("dataset", "sensor_key"), copy = TRUE) |>
    select(sequence, date, value) |>
    collect() |>
    as_tsibble(key = sequence, index = date) |>
    fill_gaps()

with_seed(0L, {
    ggplot(data) +
        geom_line(aes(x = date, y = value), na.rm = TRUE) +
        facet_grid(vars(sequence)) +
        scale_y_continuous(breaks = c(10, 30)) +
        labs(x = "Data", y = "Temperatura massima [°C]") +
        theme(strip.text.y = element_text(angle = 0))
    ggsave(fs::path(image_dir, "reggio_series_plot.tex"), width = 13.5, height = 13.5 * 0.618, units = "cm", device = tikz)
})

library(tidygraph)
library(ggraph)

breaks <- c(0, 300, 1000, 11000)
labels <- c("[0, 0.3)", "[0.3, 1)", "[1, 11)")
edges <- openxlsx::read.xlsx("elaborato/chapters/stima_normali/raccolta_dati/tagged_analysis.xlsx") |>
    as_tibble() |>
    filter(variable == 1, tag_same_series) |>
    mutate(
        display_dataset_x = if_else(dataset_x == "ISAC", network_x, dataset_x),
        display_dataset_y = if_else(dataset_y == "ISAC", network_y, dataset_y),
        from = str_c(display_dataset_x, sensor_key_x, sep = "/"),
        to = str_c(display_dataset_y, sensor_key_y, sep = "/"),
        binned_distance = cut(distance, breaks = breaks, labels = labels, right = FALSE),
        f0 = coalesce(f0, 0)
    )
graph <- tbl_graph(reggio_series, edges, directed = FALSE, node_key = "sequence")

# best: 24
# TODO: trovare un seed migliore
with_seed(24L, {
    ggraph(graph, layout = "fr") +
        geom_edge_link(aes(edge_color = f0, edge_linetype = binned_distance)) +
        geom_node_point(aes(color = display_dataset)) +
        geom_node_label(aes(label = name, color = display_dataset), repel = TRUE, seed = 0L, size = 3) +
        theme_graph(base_family = "", border = TRUE) +
        labs(color = "Dataset", shape = "Dataset", edge_linetype = "Distanza [km]") +
        scale_edge_linetype_manual(values = c("[0, 0.3)" = "solid", "[0.3, 1)" = "dotdash", "[1, 11)" = "dotted"))

    ggsave(fs::path(image_dir, "reggio_series_graph.pdf"), width = 10, height = 5)
})

library(knitr)
options(knitr.kable.NA = "")
col.names <- c(
    "Sequenza 1",
    "Sequenza 2",
    "Distanza [m]",
    "∆ Quota [m]",
    "$\\mathrm{f}_0$",
    "$\\langle \\Delta T \\rangle$ [°C]",
    "$\\langle \\lvert \\Delta T \\rvert \\rangle$ [°C]",
    "b",
    "$\\mathrm{n_d}$"
)
tagged |>
    mutate(sequence_x = str_c(dataset_x, sensor_key_x, sep = "/"), sequence_y = str_c(dataset_y, sensor_key_y, sep = "/")) |>
    select(starts_with("sequence"), distance, delH, f0, delT, maeT, balance, valid_days_inters) |>
    arrange(desc(valid_days_inters)) |>
    slice_head() |>
    kable(col.names = col.names, digits = c(0, 0, 0, 0, 2, 2, 2, 2, 0))
