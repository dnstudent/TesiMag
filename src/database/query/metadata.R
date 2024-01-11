library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

query_metadata <- function(statconn) {
    tbl(statconn, "station_geo")
}

query_duck_metadata <- function(dataconn) {
    tbl(dataconn, "stations_tmp")
}
