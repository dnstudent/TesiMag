library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)
library(purrr, warn.conflicts = FALSE)
library(tsibble, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/load/read/BRUN.R")
source("src/load/read/DPC.R")
source("src/load/read/SCIA.R")
source("src/load/ITA.R")
source("src/load/load_utils.R")

read.series <- compose(\(t) as_tsibble(t, key = identifier, index = date), read.data("series"))
load.series <- compose(\(t) as_tsibble(t, key = identifier, index = date), load.data("series"))

read.series.single <- function(db, tvar, id, ...) {
  do.call(paste("read", db, "series.single", sep = "."), list(tvar, id, ...)) |> as_tsibble(index = date)
}
read.series.bunch <- function(db, tvar, ids, ...) {
  do.call(paste("read", db, "series.bunch", sep = "."), list(tvar, ids, ...)) |> as_tsibble(key = identifier, index = date)
}

# is.scia <- is.numeric
# is.dpc <- is.character
# load.series.named <- function(tvar, ...) {
#   stations <- list(...)
#   reqs <- tibble(
#     db = map(stations, \(s) ifelse(is.scia(s), "SCIA", "DPC")) |> as.character(),
#     identifier = as.character(stations)
#   )
#   scia <- load.series("SCIA", tvar, internal_id %in% (reqs |> filter(db == "SCIA") |> pull(identifier) |> as.integer())) |>
#     mutate(station = as.character(identifier), .keep = "unused") |>
#     as_tibble()
#   if (filter(reqs, db == "DPC") |> nrow() < 1) {
#     dpc <- tibble()
#   } else {
#     dpc <- reqs |>
#       filter(db == "DPC") |>
#       rowwise() |>
#       reframe(data = load.BRUN.series.single(tvar, identifier) |> as_tibble()) |>
#       unnest(data)
#   }
#   bind_rows(
#     scia = scia,
#     dpc = dpc,
#     .id = "db"
#   ) |>
#     mutate(db = as.factor(db)) |>
#     as_tsibble(index = date, key = c(db, station))
# }

read.metadata <- read.data("metadata")
load.metadata <- compose(\(data) st_as_sf(data, coords = c("lon", "lat"), crs = "EPSG:4326"), load.data("metadata"))

load.metadata.allvars <- function(db, ..., .cache_kwargs = list()) {
  bind_rows(
    T_MIN = load.metadata(db, "T_MIN", ..., .cache_kwargs = .cache_kwargs),
    T_MAX = load.metadata(db, "T_MAX", ..., .cache_kwargs = .cache_kwargs),
    .id = "tvar"
  ) |> mutate(tvar = factor(tvar, levels = c("T_MIN", "T_MAX"), ordered = TRUE))
}

load.metadata.all <- function(..., .cache_kwargs = list()) {
  bind_rows(
    SCIA = load.metadata.allvars("SCIA", .cache_kwargs = .cache_kwargs),
    DPC = load.metadata.allvars("DPC", ..., .cache_kwargs = .cache_kwargs),
    .id = "db"
  )
}

load.series.fromnet <- function(tvar, net_name, ...) {
  identifiers <- load.metadata("SCIA", tvar) |>
    filter(rete == net_name) |>
    pull(identifier)
  load.series("SCIA", tvar, ...) |> filter(identifier %in% identifiers)
}
