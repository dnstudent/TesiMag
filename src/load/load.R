library(dplyr)
library(tidyr)
library(lubridate)
library(sf)

source("paths.R")
source("load.BRUN.R")
source("load.DPC.R")
source("load.SCIA.R")
source("load.ITA.R")
source("load_utils.R") #  MUST BE LOADED LAST

load.series <- load.data("series")

load.series.single <- function(db, tvar, id) {
  do.call(paste("load", db, "series.single", sep = "."), list(tvar, id))
}

retrieve.series <- function(db, tvar, ...) {
  retrieve.data("series", db, tvar, ...) |> as_tsibble(index = date, key = identifier)
}

is.scia <- is.numeric
is.dpc <- is.character
load.series.named <- function(tvar, ...) {
  stations <- list(...)
  reqs <- tibble(
    db = map(stations, \(s) ifelse(is.scia(s), "SCIA", "DPC")) |> as.character(),
    identifier = as.character(stations)
  )
  scia <- load.series("SCIA", tvar, internal_id %in% (reqs |> filter(db == "SCIA") |> pull(identifier) |> as.integer())) |>
    mutate(station = as.character(identifier), .keep = "unused") |>
    as_tibble()
  if (filter(reqs, db == "DPC") |> nrow() < 1) {
    dpc <- tibble()
  } else {
    dpc <- reqs |>
      filter(db == "DPC") |>
      rowwise() |>
      reframe(data = load.BRUN.series.single(tvar, identifier) |> as_tibble()) |>
      unnest(data)
  }
  bind_rows(
    scia = scia,
    dpc = dpc,
    .id = "db"
  ) |>
    mutate(db = as.factor(db)) |>
    as_tsibble(index = date, key = c(db, station))
}

load.metadata <- load.data("metadata")

retrieve.metadata <- function(db, tvar, ...) {
  retrieve.data("metadata", db, tvar, ...) |> st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326")
}

retrieve.metadata.allvars <- function(db, ...) {
  bind_rows(
    T_MIN = retrieve.metadata(db, "T_MIN", ...),
    T_MAX = retrieve.metadata(db, "T_MAX", ...),
    .id = "tvar"
  )
}

retrieve.metadata.all <- function(...) {
  bind_rows(
    SCIA = retrieve.metadata.allvars("SCIA", ...),
    DPC = retrieve.metadata.allvars("DPC", ...),
    .id = "db"
  )
}

retrieve.series.fromnet <- function(tvar, net_name, ...) {
  identifiers <- retrieve.metadata("SCIA", tvar) |>
    filter(rete == net_name) |>
    pull(identifier)
  retrieve.series("SCIA", tvar, ...) |> filter(identifier %in% identifiers)
}
