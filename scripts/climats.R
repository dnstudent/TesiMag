library(dplyr)
library(arrow)
library(vroom)
library(tidyr)
library(stringr)
library(assertr)
library(ggplot2)
library(purrr)
library(broom)
library(units)

root <- fs::path_abs("/Users/davidenicoli/Local_Workspace/Datasets/climats")
ftmin <- fs::path(root, "CLINO_GRID_ITA_TMN_v00REC_vs_OBS")
ftmax <- fs::path(root, "CLINO_GRID_ITA_TMX_v00REC_vs_OBS")
col_names <- c("name", "lon", "lat", "elevation", "slope", "orientation", "dsea", paste0("rec_", 1:12), paste0("obs_", 1:12))
col_types <- paste0(c("c", rep("d", length(col_names) - 1L)), collapse = "")
load_climats <- function(path) {
    table <- vroom::vroom_fwf(
        path,
        col_select = seq_along(col_names),
        col_types = col_types,
        col_positions = fwf_empty(path, skip = 0, n = 7000, col_names = col_names),
        show_col_types = FALSE
    ) |>
        separate_wider_regex(name, patterns = c("mesTM(?:N|X)D", "_", dataset = ".+?", "_", series_key = "\\d+", "\\.dgf"), too_few = "error") |>
        mutate(series_key = as.integer(series_key), dataset = factor(dataset), orientation = na_if(orientation, -1)) |>
        assert(not_na, dataset, series_key, lon, lat, elevation, slope) |>
        verify(between(slope, 0, 1)) |>
        verify(between(orientation, 0, 2 * pi) | is.na(orientation)) |>
        verify(between(elevation, -10, 4800)) |>
        verify(between(lon, 2.46, 20.74)) |>
        verify(between(lat, 35.50, 49.61))
    meta <- table |>
        select(dataset, series_key, lon, lat, elevation, slope, orientation, dsea)
    data <- table |>
        select(-lon, -lat, -elevation, -slope, -orientation, -dsea) |>
        pivot_longer(cols = 3:26, names_sep = "_", names_to = c("kind", "month")) |>
        mutate(month = as.integer(month), kind = factor(kind))

    list(meta = meta, data = data)
}

dbmin <- load_climats(ftmin)
dbmax <- load_climats(ftmax)

eq_or_na <- function(x, y) {
    e <- x == y
    e | is.na(e)
}
meta <- dbmin$meta |>
    full_join(dbmax$meta, by = c("dataset", "series_key")) |>
    verify(eq_or_na(lon.x, lon.y)) |>
    verify(eq_or_na(lat.x, lat.y)) |>
    verify(eq_or_na(elevation.x, elevation.y)) |>
    verify(eq_or_na(slope.x, slope.y)) |>
    verify(eq_or_na(orientation.x, orientation.y)) |>
    mutate(
        lon = coalesce(lon.x, lon.y),
        lat = coalesce(lat.x, lat.y),
        elevation = coalesce(elevation.x, elevation.y),
        slope = coalesce(slope.x, slope.y),
        orientation = coalesce(orientation.x, orientation.y),
        dsea = coalesce(dsea.x, dsea.y),
        .keep = "unused"
    )

data <- bind_rows(tmax = dbmax$data, tmin = dbmin$data, .id = "variable") |> mutate(variable = factor(variable))

write_parquet(meta, fs::path(root, "meta.parquet"))
write_parquet(data, fs::path(root, "data.parquet"))

meta |>
    sf::st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326") |>
    ggplot() +
    geom_sf()

data |>
    left_join(meta |> select(dataset, series_key, elevation), by = c("dataset", "series_key")) |>
    nest(data = c(-variable, -kind, -month)) |>
    mutate(fit = map(data, ~ lm(value ~ elevation, data = .x)), summary = map(fit, tidy)) |>
    unnest(summary) |>
    filter(term == "elevation") |>
    mutate(estimate = estimate * 1000) |>
    filter(variable == "tmin", kind == "obs") |>
    ggplot() +
    geom_histogram(aes(estimate, color = factor(month))) +
    facet_wrap(~month, ncol = 4L, scales = "free")


data |>
    filter(variable == "tmin", kind == "obs") |>
    left_join(meta |> select(dataset, series_key, elevation), by = c("dataset", "series_key")) |>
    ggplot(aes(elevation, value, group = month)) +
    geom_point() +
    geom_smooth(method = "lm", se = TRUE) +
    facet_wrap(~month, ncol = 4L)

data |>
    filter(variable == "tmin") |>
    ggplot() +
    geom_histogram(aes(value)) +
    facet_grid(month ~ kind, scales = "free")
ggsave("data.pdf", width = 10, height = 4, dpi = 300)
