library(ggplot2)
library(lubridate)
library(forcats)

source("src/database/startup.R")
source("src/database/query/data.R")
source("scripts/common.R")

theme_set(theme_bw())
linetype_values <- c(linetype_values, regionale = "solid")

conns <- load_dbs()
on.exit(close_dbs(conns))
regional_boundaries <- load_regional_boundaries(conns)

dotenv::load_dot_env("scripts/.env")
image_dir <- fs::path(Sys.getenv("IMAGES_DIR"), "datasets")
if (!fs::dir_exists(image_dir)) {
    fs::dir_create(image_dir)
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

ggplot(bind_rows(mavails, mmavails) |> filter(variable == 1L, between(year(date), 1990L, 2020L), !is.na(district), is_month_available)) +
    geom_line(aes(date, n, color = dataset, linetype = dataset)) +
    facet_wrap(~district, ncol = 3L) +
    scale_linetype_manual(values = linetype_values)
ggsave(fs::path(image_dir, "regional_monthly_availability.pdf"), width = 10, height = 10, dpi = 300)
