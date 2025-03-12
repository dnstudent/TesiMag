library(dplyr)
library(ggplot2)
library(ggrepel)
library(lubridate)
library(stringr)
library(arrow)
library(OpenStreetMap)
library(sf)
library(tsibble)
source("src/database/startup.R")
source("src/database/query/data.R")
source("notebooks/ds_regionali/procedure/common_steps.R")

dotenv::load_dot_env(fs::path("scripts", ".env"))
image_dir <- fs::path(Sys.getenv("IMAGES_DIR"), "stima_normali", "raccolta_dati")
if (!fs::dir_exists(image_dir)) {
    fs::dir_create(image_dir)
}

conns <- load_dbs()

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

autoplot(sa_map) +
    geom_point(data = stat_points, aes(color = dataset, shape = dataset), size = 7) +
    geom_label_repel(data = stat_points, aes(label = sequence), force = 1, point.padding = 10) +
    scale_color_brewer(palette = "Dark2") +
    theme(plot.margin = grid::unit(c(0, 0, 0, 0), "mm"))
ggsave(fs::path(image_dir, "reggio_series_osm.pdf"))



# Series
meta <- reggio_series |>
    st_drop_geometry() |>
    select(display_dataset, dataset, sensor_key, sequence, name) |>
    filter(
        (dataset == "Dext3r" & sensor_key %in% c(446L, 447L, 448L)) |
            (display_dataset == "ISAC") |
            (dataset == "SCIA" & sensor_key %in% c(3183L, 3276L, 3184L))
    )
# mutate(sequence = str_c(display_dataset, name, sep = "/"))
data <- query_checkpoint_data(c("Dext3r", "ISAC", "SCIA"), "raw", conns$data) |>
    filter(variable == 1L, between(date, "1990-01-01", "2020-12-31")) |>
    inner_join(meta, by = c("dataset", "sensor_key"), copy = T) |>
    select(sequence, date, value) |>
    collect() |>
    as_tsibble(key = sequence, index = date) |>
    fill_gaps()
ggplot(data) +
    geom_line(aes(x = date, y = value), na.rm = T) +
    facet_grid(vars(sequence)) +
    scale_y_continuous(breaks = c(10, 30)) +
    labs(x = "Data", y = "Temperatura massima [°C]") +
    theme(strip.text.y = element_text(angle = 0))
ggsave(fs::path(image_dir, "reggio_series_plot.pdf"), width = 10, height = 5)

library(igraph)
library(tidygraph)
library(ggraph)
source("src/merging/combining.R")

breaks <- c(0, 300, 1000, 11000)
labels <- c("[0, 300)", "[300, 1000)", "[1000, 11000)")
edges <- openxlsx::read.xlsx("elaborato/chapters/stima_normali/raccolta_dati/tagged_analysis.xlsx") |>
    as_tibble() |>
    filter(variable == 1, tag_same_series) |>
    mutate(
        display_dataset_x = if_else(dataset_x == "ISAC", network_x, dataset_x),
        display_dataset_y = if_else(dataset_y == "ISAC", network_y, dataset_y),
        from = str_c(display_dataset_x, sensor_key_x, sep = "/"),
        to = str_c(display_dataset_y, sensor_key_y, sep = "/"),
        binned_distance = cut(distance, breaks = breaks, labels = labels, right = F),
        f0 = coalesce(f0, 0)
    )
graph <- tbl_graph(reggio_series, edges, directed = F, node_key = "sequence")

ggraph(graph, layout = "fr") +
    geom_edge_link(aes(edge_color = f0, edge_linetype = binned_distance)) +
    geom_node_point(aes(color = display_dataset, shape = display_dataset)) +
    geom_node_label(aes(label = name, color = display_dataset), repel = T, seed = 0L, size = 3) +
    theme_graph(base_family = "") +
    labs(color = "Dataset", shape = "Dataset", edge_linetype = "Distanza [m]")

ggsave(fs::path(image_dir, "reggio_series_graph.pdf"), width = 10, height = 5)

# V(graph)$color <- str_match(names(V(graph)), regex("(?<dataset>.+)/\\d+")) |>
#     as_tibble(.name_repair = "universal") |>
#     left_join(colors, by = "dataset") |>
#     pull(color)

# pdf(fs::path(image_dir, "reggio_series_graph.pdf"), width = 10, height = 5)
# graph |> plot()
# dev.off()

library(knitr)
options(knitr.kable.NA = "")
tagged |>
    mutate(sequence_x = str_c(dataset_x, sensor_key_x, sep = "/"), sequence_y = str_c(dataset_y, sensor_key_y, sep = "/")) |>
    select(starts_with("sequence"), distance, delH, f0, delT, maeT, balance, valid_days_inters) |>
    arrange(desc(valid_days_inters)) |>
    slice_head() |>
    kable(col.names = c("Sequenza 1", "Sequenza 2", "Distanza [m]", "∆ Quota [m]", "$\\mathrm{f}_0$", "$\\langle \\Delta T \\rangle$ [°C]", "$\\langle \\lvert \\Delta T \\rvert \\rangle$ [°C]", "b", "$\\mathrm{n_d}$"), digits = c(0, 0, 0, 0, 2, 2, 2, 2, 0))


close_dbs(conns)
