library(ggplot2)
library(lubridate)
library(forcats)
library(tikzDevice)
library(withr)
source("src/database/startup.R")
source("src/database/query/data.R")
source("scripts/common.R")

loadfonts()
theme_set(theme_bw() + theme_defaults)

linetype_values <- c(linetype_values, regionale = "solid")

conns <- load_dbs()
on.exit(close_dbs(conns))
regional_boundaries <- load_regional_boundaries(conns)

options(tikzDefaultEngine = "xetex")
dotenv::load_dot_env("scripts/.env")
image_dir <- fs::path(Sys.getenv("IMAGES_DIR"), "datasets")
if (!fs::dir_exists(image_dir)) {
    fs::dir_create(image_dir)
}
table_dir <- fs::path(Sys.getenv("TABLES_DIR"), "datasets")
if (!fs::dir_exists(table_dir)) {
    fs::dir_create(table_dir)
}
pres_dir <- fs::path(Sys.getenv("PRES_DIR"), "composizione_dataset")
if (!fs::dir_exists(pres_dir)) {
    fs::dir_create(pres_dir)
}

monthly_availability_by_series <- function(data, ...) {
    data |>
        filter(!is.na(value)) |>
        group_by(..., month = as.integer(month(date)), year = as.integer(year(date))) |>
        mutate(pdatediff = as.integer(date - lag(date, order_by = date)) |> coalesce(1L)) |>
        summarise(is_month_available = (n() >= 20L) & (max(pdatediff, na.rm = TRUE) <= 4L), .groups = "drop")
}

monthly_availability <- function(mavails, ...) {
    mavails |>
        count(..., year, month, is_month_available) |>
        mutate(date = make_date(year, month, 1L), .keep = "unused")
}

nat_regioni <- tibble(
    dataset = c(rep("ISAC", length(arpas_ds)), rep("SCIA", length(arpas_ds)), rep("DPC", length(arpas_ds))),
    district = rep(names(arpas_ds), 3)
)
arpas_regioni <- tibble(dataset = c(arpas_ds), district = names(arpas_ds)) |> bind_rows(nat_regioni)
metas <- load_raw_metas(c(na.omit(arpas_ds), "ISAC", "SCIA"), conns, regional_boundaries) |>
    filter(country == "Italy") |>
    semi_join(arpas_regioni, by = colnames(arpas_regioni))
datas <- load_raw_datas(c(na.omit(arpas_ds), "ISAC", "SCIA"), conns, metas, regional_boundaries)
mmetas <- load_merged_meta(conns, regional_boundaries) |> filter(country == "Italy")
mdatas <- load_merged_data(conns, mmetas, regional_boundaries)


mavails <- datas |>
    monthly_availability_by_series(dataset, sensor_key, variable) |>
    left_join(metas |> select(dataset, sensor_key, district), by = c("dataset", "sensor_key"), copy = TRUE) |>
    monthly_availability(dataset, district, variable) |>
    collect() |>
    mutate(dataset = factor(dataset) |> fct_other(keep = c("SCIA", "ISAC", "DPC"), other_level = "regionale"))
mmavails <- mdatas |>
    monthly_availability_by_series(dataset, sensor_key, variable) |>
    left_join(mmetas |> select(dataset, sensor_key, district), by = c("dataset", "sensor_key"), copy = TRUE) |>
    monthly_availability(district, variable) |>
    mutate(dataset = "merged") |>
    collect()

with_seed(0L, {
    ggplot(bind_rows(mavails, mmavails) |> filter(variable == 1L, between(year(date), 1990L, 2020L), !is.na(district), is_month_available)) +
        geom_line(aes(date, n, color = dataset, linetype = dataset)) +
        facet_wrap(~district, ncol = 3L) +
        scale_linetype_manual(values = linetype_values) +
        labs(x = "Data")
    ggsave(fs::path(image_dir, "regional_monthly_availability.tex"), width = 13.5, height = 16, units = "cm", device = tikz)
})

with_seed(0L, {
    ggplot(bind_rows(mavails, mmavails) |> filter(variable == 1L, !(dataset %in% c("ISAC")), between(year(date), 1991L, 2020L), !is.na(district), is_month_available, district %in% c("Piemonte", "Friuli-Venezia Giulia", "Veneto"))) +
        geom_line(aes(date, n, color = dataset)) +
        facet_wrap(~district, ncol = 6L) +
        theme(axis.text.x = element_text(angle = 30), axis.title.x = element_blank(), axis.title.y = element_blank(), legend.margin = margin(l = -0.1, unit = "cm"), legend.title = element_blank(), plot.margin = margin(0, 0, 0, 0))
    ggsave(fs::path(pres_dir, "sources_availability.tex"), width = 12, height = 3.2, units = "cm", device = tikz)
})


library(openxlsx)
library(knitr)
library(kableExtra)
read.xlsx("/Users/davidenicoli/Local_Workspace/TesiMag/elaborato/tables/datasets/datasets.xlsx") |>
    as_tibble() |>
    select(1L, 2L, 3L) |>
    kbl(format = "latex", col.names = c("Ente", "Sorgente", "Note"), booktabs = TRUE) |>
    cat(file = fs::path(table_dir, "regio.tex"))
