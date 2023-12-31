library(DBI, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(duckdb, warn.conflicts = FALSE)
library(RPostgres, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)

source("src/load/tools.R")

add_ds_info <- function(infos) {
    conn <- dbConnect(Postgres(), dbname = "georefs")
    name <- infos[[1]]
    source <- infos[[2]]
    kind <- infos[[3]]
    description <- infos[[4]]
    dbExecute(conn, str_glue("INSERT INTO dataset (name, source, kind, description) VALUES ('{name}', '{source}', '{kind}', '{description}') ON CONFLICT (name) DO NOTHING"))
    dbDisconnect(conn)
}

insert_dataset <- function(connquack, connpost, dataset) {
    stations <- select(dataset$meta, all_of(station_headers))
    dbAppendTable(connpost, stations, "station")
    # stats_ids <- tbl(connpost, "station") |>
    #     semi_join(stations, by = c("original_id", "dataset_name")) |>
    #     select(id, original_id, dataset_name) |>
    #     collect()

    # dbCreateTable(connpost, )



    boundaries <- st_read(conn, query = "SELECT name, ST_AsWKB(geom) AS geom FROM boundary WHERE kind = 'state'", geometry_column = "geom", crs = 4326)
    stations <- dataset$meta |>
        st_md_to_sf(remove = FALSE)
    substitute_states <- st_join(stations |> select(-state), boundaries |> rename(state = name), st_within)
    stations <- stations |>
        mutate(state = coalesce(state, substitute_states$state)) |>
        st_drop_geometry()
    duckdb_register(conn, "stations_to_add", stations)

    mt <- dbExecute(
        conn,
        "
        WITH metadata AS (SELECT *, ST_Point(lon, lat)::GEOMETRY AS geom FROM stations_to_add)

        INSERT OR IGNORE INTO station (id, dataset_name, network, lon, lat, elevation, geom, name, first_registration, last_registration, state, original_id)
        SELECT
            hash(concat(dataset_name, original_id)) AS id,
            dataset_name,
            network,
            lon,
            lat,
            elevation,
            geom,
            name,
            first_registration,
            last_registration,
            state,
            original_id
        FROM metadata
        "
    )
    duckdb_unregister(conn, "stations_to_add")

    duckdb_register_arrow(conn, "data_view", dataset$data)
    dbExecute(
        conn,
        "
        INSERT OR IGNORE INTO raw_daily_measure (station_id, variable, date, value)
        SELECT s.id AS station_id, d.variable, d.date, d.value
        FROM data_view d
        JOIN station s
        USING (original_id, dataset_name)
        ORDER BY station_id, variable, date
        "
    )
    duckdb_unregister_arrow(conn, "data_view")
}
