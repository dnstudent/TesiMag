library(arrow)
library(dplyr)
library(tidyr)
library(lubridate)
library(sf)
library(stars)
library(terra)
library(tibble)
library(stringr)
library(vroom)

source("paths.R")
source("load.DPC.R")

load.DEM.COP30 <- function() {
  ls.COP30() |>
    st_mosaic() |>
    read_stars()
}

missing_coords <- list(
  c(5, 37),
  c(5, 38),
  c(5, 39),
  c(5, 40),
  c(5, 41),
  c(5, 42),
  c(6, 38),
  c(6, 39),
  c(6, 40),
  c(6, 41),
  c(7, 38),
  c(7, 39),
  c(7, 40),
  c(7, 41),
  c(7, 42),
  c(10, 38),
  c(10, 39),
  c(10, 40),
  c(10, 41),
  c(11, 38),
  c(11, 39),
  c(11, 40),
  c(12, 39),
  c(13, 39),
  c(13, 35),
  c(13, 36),
  c(14, 39),
  c(14, 43),
  c(18, 41),
  c(15, 35),
  c(16, 35),
  c(16, 36),
  c(17, 35),
  c(17, 36),
  c(17, 37),
  c(18, 35),
  c(18, 36),
  c(18, 37),
  c(18, 38),
  c(19, 35),
  c(19, 36),
  c(19, 37),
  c(19, 38),
  c(20, 35),
  c(20, 36)
)

build.DEM.COP30.sea_mask <- function() {
  files <- c(ls.COP30.WBM(), ls.COP30.WBM.missing())
  wbm_data <- vrt(files)
  sea <- wbm_data == 1
  sea[!sea] <- NA
  sea
}


load.SCIA.series <- function(tvar, ...) {
  read_parquet(path.series("SCIA", tvar), as_data_frame = TRUE) |>
    filter(...) |>
    rename_with(\(x) tvar, starts_with("Temperatura"))
}

load.SCIA.series.single <- function(var, id) {
  load.SCIA.series(var, internal_id == as.integer(id))
}

load.SCIA.metadata <- function(tvar) {
  stations <- read_parquet(path.stations("SCIA", tvar))
  corrections <- read_parquet(path.SCIA.stations.corrections(tvar))
  # Missing values in coordinates are not allowed! Station hight must be provided
  left_join(stations, corrections, by = "internal_id") |> st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326")
}



widths <- c(5, 3, rep(7, 31))

load.DPC.series.single <- function(tvar, name) {
  vroom::vroom_fwf(file.path(path.section("DPC", tvar), name),
    fwf_widths(widths, col_names = c("year", "month", seq.int(1, 31))),
    col_types = cols(year = "i", month = "i", .default = "d"),
    na = c("", "NA", "-90.0"), altrep = FALSE, id = "file", progress = FALSE
  ) |>
    mutate(station = factor(str_split_i(file, "/", -1)), .keep = "unused", .before = 1) |>
    pivot_longer(cols = seq(4, 34), names_to = "day", values_to = tvar, names_transform = as.integer) |>
    mutate(date = make_date(year, month, day), .keep = "unused")
}

load.DPC.series <- function(tvar, ...) {
  vroom::vroom_fwf(path.series("DPC", tvar),
    fwf_widths(widths, col_names = c("year", "month", seq.int(1, 31))),
    col_types = cols(year = "i", month = "i", .default = "d"),
    na = c("", "NA", "-90.0"), altrep = FALSE, id = "file", progress = FALSE
  ) |>
    mutate(station = factor(str_split_i(file, "/", -1)), .keep = "unused", .before = 1) |>
    pivot_longer(cols = seq(4, 34), names_to = "day", values_to = tvar, names_transform = as.integer) |>
    mutate(date = make_date(year, month, day), .keep = "unused") |>
    filter(...)
}

load.DPC.metadata <- function(tvar, cache = "temp/metadata/DPC_metadata_{tvar}.parquet") {
  data <- NULL
  if (!is.null(cache)) {
    cache <- str_glue(cache)
    if (file.exists(cache)) {
      data <- read_parquet(cache)
    } else {
      message("Cache not found, loading from disk and writing to cache")
      data <- load.DPC.stations(tvar)
      write_parquet(data, cache)
    }
  } else {
    data <- load.DPC.stations(tvar)
  }
  data |> st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326")
}

load.series <- function(network, tvar, ...) {
  if (network == "SCIA") {
    return(load.SCIA.series(tvar, ...))
  } else if (network == "DPC") {
    return(load.DPC.series(tvar, ...))
  } else {
    stop("Invalid network name")
  }
}

is.scia <- is.numeric
is.dpc <- is.character
load.series.named <- function(tvar, ...) {
  stations <- list(...)
  reqs <- tibble(
    db = map(stations, \(s) ifelse(is.scia(s), "SCIA", "DPC")) |> as.character(),
    identifier = as.character(stations)
  )
  bind_rows(
    scia = load.series("SCIA", tvar, internal_id %in% (reqs |> filter(db == "SCIA") |> pull(identifier) |> as.integer())) |> mutate(station = as.character(internal_id), .keep = "unused"),
    dpc = reqs |>
      filter(db == "DPC") |>
      rowwise() |>
      reframe(data = load.DPC.series.single(tvar, identifier)) |>
      unnest(data),
    .id = "db"
  ) |> mutate(db = as.factor(db))
}

load.metadata <- function(network, tvar, ...) {
  if (network == "SCIA") {
    return(load.SCIA.metadata(tvar))
  } else if (network == "DPC") {
    return(load.DPC.metadata(tvar, ...))
  } else {
    stop("Invalid network name")
  }
}

load.italy <- function() {
  st_read(path.boundaries.italy.states)
}
