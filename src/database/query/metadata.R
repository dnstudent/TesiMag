library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

query_metadata <- function(statconn) {
    tbl(statconn, "raw_station_geo")
}

query_duck_metadata <- function(dataconn) {
    tbl(dataconn, "raw_stations_tmp")
}
