library(DBI, warn.conflicts = FALSE)
library(duckdb, warn.conflicts = FALSE)
library(RPostgres, warn.conflicts = FALSE)

load_dbs <- function() {
    connpost <- dbConnect(Postgres(), dbname = "georefs", user = "davidenicoli", host = "localhost")
    connquack <- dbConnect(duckdb())
    dbExecute(connquack, "PRAGMA temp_directory='db/tmp/duckdb'")
    dbExecute(connquack, "PRAGMA max_temp_directory_size='100GiB'")
    dbExecute(
        connquack,
        "
        INSTALL postgres;
        LOAD postgres;
        INSTALL icu;
        LOAD icu;
        -- ATTACH 'dbname=georefs' AS postgis_db (TYPE postgres);
        -- CREATE OR REPLACE TEMPORARY TABLE raw_stations_tmp AS SELECT * EXCLUDE (geom, geog) FROM postgis_db.raw_station_geo;
        "
    )
    list(stations = connpost, data = connquack)
}

close_dbs <- function(conns) {
    dbDisconnect(conns$stations)
    dbDisconnect(conns$data)
}
