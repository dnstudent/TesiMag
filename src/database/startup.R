library(DBI, warn.conflicts = FALSE)
library(duckdb, warn.conflicts = FALSE)
library(RPostgres, warn.conflicts = FALSE)

# init_dbs <- function() {
#     connquack <- dbConnect(duckdb(), "db/series.duckdb")
#     dbExecute(
#         connquack,
#         "
#     INSTALL spatial;
#     LOAD spatial;
#     CREATE TYPE tvar AS ENUM ('T_MIN', 'T_MAX');
#     CREATE TYPE dskind AS ENUM ('national', 'regional');
#     CREATE TYPE rgkind AS ENUM ('province', 'state', 'country');
#     CREATE TABLE IF NOT EXISTS dataset (
#         name VARCHAR PRIMARY KEY,
#         source VARCHAR,
#         kind dskind NOT NULL,
#     );
#     CREATE TABLE IF NOT EXISTS boundary (
#         name VARCHAR PRIMARY KEY,
#         kind rgkind NOT NULL,
#         state VARCHAR NOT NULL,
#         geom GEOMETRY NOT NULL,
#     );
#     CREATE TABLE IF NOT EXISTS station (
#         id LONG PRIMARY KEY,
#         dataset_name VARCHAR NOT NULL,
#         network VARCHAR NOT NULL,
#         lon DOUBLE NOT NULL CHECK (3.5 <= lon AND lon <= 21),
#         lat DOUBLE NOT NULL CHECK (35 <= lat AND lat <= 48.5),
#         elevation DOUBLE CHECK (-10 <= elevation AND elevation <= 4810),
#         geom GEOMETRY NOT NULL,
#         name VARCHAR,
#         first_registration DATE NOT NULL,
#         last_registration DATE NOT NULL,
#         boundary_name VARCHAR NOT NULL,
#         original_id VARCHAR NOT NULL,
#         FOREIGN KEY (dataset_name) REFERENCES dataset (name),
#         FOREIGN KEY (boundary_name) REFERENCES boundary (name),
#     );
#     CREATE TABLE IF NOT EXISTS raw_daily_measure (
#         station_id LONG NOT NULL,
#         variable tvar NOT NULL,
#         date DATE NOT NULL CHECK(make_date(2000, 1, 1) <= date AND date < make_date(2023, 1, 1)),
#         value DOUBLE NOT NULL,
#         original BOOL NOT NULL,
#         FOREIGN KEY (station_id) REFERENCES station (id),
#     );
#     CREATE TABLE IF NOT EXISTS station_match (
#         id_x LONG NOT NULL,
#         id_y LONG NOT NULL,
#         mergeable BOOL NOT NULL,
#         FOREIGN KEY (id_x) REFERENCES station (id),
#         FOREIGN KEY (id_y) REFERENCES station (id),
#     );
#     "
#     )
#     dbDisconnect(connquack)
# }

load_dbs <- function() {
    connpost <- dbConnect(Postgres(), dbname = "georefs", user = "davidenicoli", host = "localhost")
    connquack <- dbConnect(duckdb())
    dbExecute(connquack, "PRAGMA temp_directory='db/tmp'")
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
