# %%
library(conflicted, warn.conflicts = FALSE, quietly = TRUE)
library(arrow, warn.conflicts = FALSE, quietly = TRUE)
library(dplyr, warn.conflicts = FALSE, quietly = TRUE)
library(ggplot2, warn.conflicts = FALSE, quietly = TRUE)
library(ggbreak, warn.conflicts = FALSE, quietly = TRUE)
library(patchwork, warn.conflicts = FALSE, quietly = TRUE)
library(stars, warn.conflicts = FALSE, quietly = TRUE)
library(sf, warn.conflicts = FALSE, quietly = TRUE)
library(units, warn.conflicts = FALSE, quietly = TRUE)
library(tidyr, warn.conflicts = FALSE, quietly = TRUE)
library(repr, warn.conflicts = FALSE, quietly = TRUE)
library(knitr, warn.conflicts = FALSE, quietly = TRUE)
library(kableExtra, warn.conflicts = FALSE, quietly = TRUE)
library(withr, warn.conflicts = FALSE, quietly = TRUE)
library(tikzDevice, warn.conflicts = FALSE, quietly = TRUE)
conflicts_prefer(dplyr::filter)
conflicts_prefer(dbplyr::sql)

source("scripts/common.R")
source("src/database/startup.R")
source("src/database/query/data.R")

# %%
#  Plotting and export settings
loadfonts()
theme_set(theme_bw() + theme_defaults)
options(tikzDefaultEngine = "xetex")
dotenv::load_dot_env("scripts/.env")
image_dir <- fs::path(Sys.getenv("IMAGES_DIR"), "valutazione_modello", "risultati")
if (!fs::dir_exists(image_dir)) {
    fs::dir_create(image_dir)
}
table_dir <- fs::path(Sys.getenv("TABLES_DIR"), "valutazione_modello", "risultati")
if (!fs::dir_exists(table_dir)) {
    fs::dir_create(table_dir)
}
tntx_scale <- c(TN = "dodgerblue", TX = "firebrick1")

# Dbs init
conns <- load_dbs()
on.exit(close_dbs(conns))

# %%

#  Data loading
dss <- c("VDA", "PIE", "LIG", "LOM", "TOS", "UMB", "MAR", "VEN", "FVG", "ER", "TAA2", "FRA", "AUT", "SLO", "SWI")
ita_dss <- c("VDA", "PIE", "LIG", "LOM", "TOS", "UMB", "MAR", "VEN", "FVG", "ER", "TAA2")
# boundaries <- load_regional_boundaries(conns)
clino_path <- fs::path("/Users/davidenicoli/Local_Workspace/Datasets/climats")
merged_metas <- load_merged_meta(conns, boundaries = NULL) |> rename(series_key = sensor_key)
meta <- query_parquet(fs::path(clino_path, "meta.parquet"), conns$data) |>
    filter(dataset %in% dss) |>
    semi_join(merged_metas, by = c("dataset", "series_key"), copy = TRUE) |>
    mutate(dseacat = cut(dsea, breaks = c(-Inf, 10, 50, Inf), labels = c("next", "close", "far")))
data <- query_parquet(fs::path(clino_path, "data.parquet"), conns$data) |> semi_join(meta, by = c("dataset", "series_key"))
geometa <- st_as_sf(collect(meta), coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE)
biases <- data |>
    pivot_wider(names_from = kind, values_from = value) |>
    mutate(BIAS = rec - obs) |>
    mutate(variable = case_match(variable, "tmax" ~ "TX", "tmin" ~ "TN", .default = variable))

# %% Table 1
bias_table <- function(biases, .by) {
    biases |>
        summarise(BIAS = mean(BIAS, na.rm = TRUE), MAE = mean(abs(BIAS), na.rm = TRUE), RMSE = sqrt(mean(BIAS^2, na.rm = TRUE)), .by = {{ .by }})
}
italian <- expr(dataset %in% c("VDA", "PIE", "LIG", "LOM", "TOS", "UMB", "MAR", "VEN", "FVG", "ER", "TAA2"))
format_table1 <- function(table) {
    table |>
        kable(digits = 2)
}

col_names <- c("Mese", rep(c("BIAS", "MAE", "RMSE"), 3L))

# Define the header structure
header <- c(" " = 1, "TN [\\\\unit{\\\\degreeCelsius}]" = 3, "TX [\\\\unit{\\\\degreeCelsius}]" = 3, "TM14\\\\tnote{*} [\\\\unit{\\\\degreeCelsius}]" = 3)

#  tab1 2014
tab2014 <- read_csv_arrow("scripts/raccolta_dati/tab2014.csv")

# tab1 ita
results <- bias_table(biases |> filter(italian), .by = c(variable, month)) |>
    pivot_wider(id_cols = month, names_from = variable, values_from = c(BIAS, MAE, RMSE)) |>
    relocate(month, BIAS_TN, MAE_TN, RMSE_TN, BIAS_TX, MAE_TX, RMSE_TX) |>
    left_join(tab2014, by = c("month" = "Mese"), copy = TRUE) |>
    arrange(month)

# TODO: aggiungere colori per semplicità di consultazione
results |>
    kbl(format = "latex", booktabs = TRUE, col.names = col_names, digits = 2L) |>
    add_header_above(header, escape = FALSE) |>
    cat(file = fs::path(table_dir, "table1_ita.tex"), append = FALSE)

bias_table(biases |> filter(!italian), .by = c(variable, month)) |>
    pivot_wider(id_cols = month, names_from = variable, values_from = c(BIAS, MAE, RMSE)) |>
    relocate(month, BIAS_TN, MAE_TN, RMSE_TN, BIAS_TX, MAE_TX, RMSE_TX) |>
    left_join(tab2014, by = c("month" = "Mese"), copy = TRUE) |>
    arrange(month) |>
    kbl(format = "latex", booktabs = TRUE, col.names = col_names, digits = 2L) |>
    add_header_above(header, escape = FALSE) |>
    cat(file = fs::path(table_dir, "table1_non_ita.tex"), append = FALSE)

# %%
maermse_plot <- function(stats) {
    stats |>
        pivot_longer(cols = c(MAE, RMSE), names_to = "stat") |>
        ggplot() +
        geom_line(aes(month, value, color = variable, linetype = stat)) +
        labs(color = "Variabile", linetype = "Metrica", x = "Mese", y = "Valore [\\textdegree C]") +
        scale_y_continuous(limits = c(0.4, 1.4))
}

bias_plot <- function(stats, label) {
    stats |>
        ggplot() +
        geom_line(aes(month, BIAS, color = variable), show.legend = FALSE) +
        labs(color = "Variabile", x = "Mese", y = "BIAS [\\textdegree C]") +
        scale_y_continuous(breaks = c(-0.05, 0, 0.05), limits = c(-0.08, 0.08))
}

with_seed(0L, {
    istats <- bias_table(biases |> filter(italian), .by = c(variable, month)) |>
        collect() |>
        bind_rows(tab2014 |> mutate(variable = "TM14") |> rename(month = Mese))
    nistats <- bias_table(biases |> filter(!italian), .by = c(variable, month)) |>
        collect() |>
        bind_rows(tab2014 |> mutate(variable = "TM14") |> rename(month = Mese))

    p2i <- maermse_plot(istats) + labs(subtitle = "Serie italiane")
    p1i <- bias_plot(istats)

    p2n <- maermse_plot(nistats) + labs(subtitle = "Serie estere")
    p1n <- bias_plot(nistats)

    (p2i + p2n + p1i + p1n) + plot_layout(nrow = 2L, ncol = 2L, heights = c(2, 0.7), widths = c(1, 1), axes = "collect", guides = "collect") + plot_annotation(title = "Errori del modello", subtitle = "Confronto Italia - estero") &
        scale_color_manual(values = c(tntx_scale, "TM14" = "black")) &
        scale_x_continuous(breaks = seq(1L, 12L, by = 2L)) &
        scale_linetype_manual(values = c("BIAS" = "solid", "MAE" = "dotdash", "RMSE" = "dashed"))
    ggsave(fs::path(image_dir, "diff_stats.tex"), width = 13.5, height = 7, units = "cm", device = tikz)
})

with_seed(0L, {
    stats <- bias_table(biases |> filter(!italian), .by = c(variable, month)) |>
        collect() |>
        bind_rows(tab2014 |> mutate(variable = "TM14") |> rename(month = Mese))
    p2 <- stats |>
        pivot_longer(cols = c(MAE, RMSE), names_to = "stat") |>
        ggplot() +
        geom_line(aes(month, value, color = variable, linetype = stat)) +
        labs(color = "Variabile", linetype = "Metrica", x = "Mese", y = "Valore [\\textdegree C]") +
        scale_color_manual(values = c(tntx_scale, "TM14" = "black")) +
        scale_x_continuous(breaks = seq(1L, 12L, by = 2L)) +
        scale_linetype_manual(values = c("MAE" = "dotdash", "RMSE" = "dashed"))
    p1 <- stats |>
        ggplot() +
        geom_line(aes(month, BIAS, color = variable), show.legend = FALSE) +
        labs(color = "Variabile", linetype = "Metrica", x = "Mese", y = "BIAS [\\textdegree C]") +
        scale_color_manual(values = c(tntx_scale, "TM14" = "black")) +
        scale_x_continuous(breaks = seq(1L, 12L, by = 2L)) +
        scale_y_continuous(n.breaks = 3L)
    p2 / p1 + plot_layout(heights = c(2, 0.7), axes = "collect", guides = "collect") & theme_quartz
    ggsave(fs::path(image_dir, "diff_stats_nonita.tex"), width = 13.5, height = 7, units = "cm", device = tikz)
})

# %%
with_seed(0L, {
    pds <- biases |>
        # filter(italian) |>
        mutate(
            dataset = case_match(dataset, "TAA2" ~ "TAA", .default = dataset)
        ) |>
        arrange(dataset) |>
        ggplot() +
        geom_hline(yintercept = 0, color = "black", size = 0.1) +
        geom_boxplot(aes(x = dataset, y = BIAS, color = variable), position = "dodge", outliers = FALSE, fill = NA) +
        labs(x = "Regione", y = "BIAS [\\textdegree C]", color = "Variabile")

    ph <- biases |>
        # filter(!italian) |>
        left_join(meta |> select(dataset, series_key, elevation), by = c("dataset", "series_key")) |>
        ggplot() +
        geom_hline(yintercept = 0, color = "black", size = 0.1) +
        geom_boxplot(
            aes(
                x = cut(elevation, breaks = c(-Inf, 500, 1000, 1500, 2000, Inf), labels = c("<500", "(500, 1000]", "(1000, 1500]", "(1500, 2000]", ">2000")),
                y = BIAS,
                color = variable
            ),
            fill = NA,
            position = "dodge",
            outliers = FALSE
        ) +
        labs(x = "Quota [m]", y = "BIAS [\\textdegree C]", color = "Variabile")

    pds + ph +
        plot_layout(guides = "collect", axes = "collect", widths = c(15L, 5L)) +
        plot_annotation(title = "Distribuzione dei BIAS del modello") &
        ylim(c(-5, 5)) &
        theme(axis.text.x = element_text(angle = 45, hjust = 1), axis.title.x = element_text(margin = margin(10))) &
        scale_color_manual(values = tntx_scale)

    ggsave(fs::path(image_dir, "bplots.tex"), width = 13.5, height = 9, units = "cm", device = tikz)
})

# %%
with_seed(0L, {
    biases |>
        # filter(italian) |>
        left_join(meta |> select(dataset, series_key, elevation), by = c("dataset", "series_key")) |>
        ggplot() +
        geom_boxplot(aes(x = cut(elevation, breaks = c(-Inf, 500, 1000, 1500, 2000, 2500, Inf), labels = c("<500", "(500, 1000]", "(1000, 1500]", "(1500, 2000]", "(2000, 2500]", ">2500")), y = BIAS, fill = variable), position = "dodge", outliers = FALSE) +
        scale_fill_manual(values = tntx_scale) +
        labs(x = "Quota [m]", y = "BIAS [\\textdegree C]", fill = "Variabile")
    # facet_grid(month ~ ., scales = "free_y")
    ggsave(fs::path(image_dir, "elevation_bplot.tex"), width = 13.5, height = 6, units = "cm", device = tikz)
})

# %%
with_seed(0L, {
    stats <- biases |>
        left_join(meta |> select(dataset, series_key, dseacat, elevation), by = c("dataset", "series_key")) |>
        filter(elevation < 500) |>
        summarise(BIAS = mean(BIAS, na.rm = TRUE), RMSE = mean(abs(BIAS^2), na.rm = TRUE), .by = c(variable, month, dseacat)) |>
        collect() |>
        mutate(dseacat = factor(dseacat, levels = c("next", "close", "far"), labels = c("Vicina (<10km)", "Intermedia (<50km)", "Lontana (>50km)")))
    pbias <- stats |>
        ggplot() +
        geom_line(aes(x = month, y = BIAS, color = variable, linetype = dseacat)) +
        labs(x = "Mese", y = "BIAS [\\textdegree C]", color = "Variabile", linetype = "Distanza dal mare")

    prmse <- stats |> ggplot() +
        geom_line(aes(x = month, y = RMSE, color = variable, linetype = dseacat)) +
        labs(x = "Mese", y = "RMSE [\\textdegree C]", color = "Variabile", linetype = "Distanza dal mare")

    pbias + prmse +
        plot_layout(guides = "collect") +
        plot_annotation(title = "Errori del modello", subtitle = "Relazione con la distanza dal mare") &
        scale_x_continuous(breaks = seq(1L, 12L, by = 2L)) &
        scale_color_manual(values = tntx_scale) &
        scale_linetype_manual(values = c("Vicina (<10km)" = "solid", "Intermedia (<50km)" = "dashed", "Lontana (>50km)" = "dotdash"))
    ggsave(fs::path(image_dir, "sea_lplot.tex"), width = 13.5, height = 6, units = "cm", device = tikz)
})

# %%
boundaries <- load_regional_boundaries(conns)
library(colorspace)
with_seed(0L, {
    biases |>
        summarise(BIAS = mean(BIAS, na.rm = TRUE), .by = c("dataset", "series_key", "variable")) |>
        group_by(dataset, series_key) |>
        filter(any(abs(BIAS) > 1.5, na.rm = TRUE)) |>
        ungroup() |>
        collect() |>
        left_join(geometa, by = c("dataset", "series_key"), copy = TRUE) |>
        filter(!!italian) |>
        st_as_sf() |>
        ggplot() +
        geom_sf(data = boundaries, fill = NA, color = "black", size = 0.2) +
        geom_sf(aes(color = BIAS), size = .5) +
        # scale_color_gradient2(low = "blue", mid = "white", high = "red", midpoint = 0) +
        scale_color_continuous_diverging(palette = "Blue-Red 2", mid = 0) +
        theme_bw() +
        labs(color = "BIAS [\\textdegree C]", x = "Latitudine [°E]", y = "Longitudine [°N]") +
        facet_grid(~variable) +
        scale_x_continuous(labels = ~.) +
        scale_y_continuous(labels = ~.)

    # ggsave(fs::path(image_dir, "bias_dist.pdf"), width = 13.5, height = 10, units = "cm")
})

# %%
monthly_availability_by_series <- function(data, ...) {
    data |>
        filter(!is.na(value)) |>
        group_by(..., month = as.integer(month(date)), year = as.integer(year(date)), .add = TRUE) |>
        mutate(pdatediff = as.integer(date - lag(date, order_by = date)) |> coalesce(1L)) |>
        summarise(is_month_available = (n() >= 20L) & (max(pdatediff, na.rm = TRUE) <= 4L), .groups = "drop")
}

merged_meta <- load_merged_meta(conns, boundaries = NULL)
merged_data <- load_merged_data(conns, merged_meta, boundaries = NULL)
availability <- merged_data |>
    filter(between(year(date), 1991L, 2020L), !is.na(value)) |>
    monthly_availability_by_series(dataset, sensor_key, variable) |>
    collect()
ydisps <- availability |>
    group_by(dataset, sensor_key, variable, month) |>
    summarise(n_mavail = sum(is_month_available), .groups = "drop_last") |>
    ungroup() |>
    mutate(variable = case_match(variable, -1L ~ "TN", 1L ~ "TX", .default = NA_character_))

things <- biases |>
    left_join(ydisps, copy = TRUE) |>
    collect() |>
    mutate(n_mavail = cut(n_mavail, breaks = c(-Inf, 6L, 12L, 18L, 24L, Inf)), italian = !!italian)
things |>
    ggplot() +
    geom_boxplot(aes(x = italian, y = BIAS, color = variable), position = "dodge") +
    scale_color_manual(values = tntx_scale) +
    # facet_grid(~italian) +
    theme_quartz

# summarise(n_mavail = min(n_mavail), .groups = "drop")
close <- function(x, y) {
    abs(x - y) < 0.00001
}
