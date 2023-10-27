SCIA.anagrafica_file.relative <- function(tvar) {
  file.path("db", "wfs_stations.parquet")
}

SCIA.root.relative <- function(tvar) {
  file.path("SCIA", "giornaliere", switch(tvar,
    T_MAX = "massime",
    T_MIN = "minime",
    stop("Invalid variable name: ", tvar)
  ))
}

SCIA.stations_corrections.relative <- function(tvar) {
  file.path("db", "station_interop.parquet")
}
