library(DBI, warn.conflicts = FALSE)
library(RPostgres, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)

query_boundary <- function(conn, name, kind) {
    st_read(conn, query = stringr::str_glue("SELECT * FROM boundary WHERE name = '{name}' AND kind = '{kind}'"), geometry_column = "geom")
}

query_state_matches <- function(statconn, state_name, distance_threshold, buffer_m, cmp = "<") {
    query <- glue::glue(
        "
        WITH inside_stations AS (SELECT s.*
        FROM raw_station_geo s
        JOIN boundary b
        ON ST_DWithin(b.geom::geography, s.geog, {buffer_m})
        WHERE b.name = '{state_name}'
        )
        SELECT
            a.id AS id_x,
            b.id AS id_y,
            a.original_dataset AS dataset_x,
            b.original_dataset AS dataset_y,
            a.network AS network_x,
            b.network AS network_y,
            ST_Distance(a.geog, b.geog) AS distance
        FROM inside_stations a
        JOIN raw_station_geo b
        ON ST_DWithin(a.geog, b.geog, {distance_threshold})
        WHERE a.id {cmp} b.id
        ",
        .con = statconn
    )
    tbl(statconn, sql(query))
}

query_stations_inside <- function(statconn, shape_name, buffer_m = 0) {
    if (buffer_m < 1e-4) {
        query <- glue::glue_sql(
            "SELECT s.*
        FROM raw_station_geo s
        JOIN boundary b
        ON ST_Contains(b.geom, s.geom)
        WHERE b.name = {shape_name}",
            .con = statconn
        )
    } else {
        query <- glue::glue_sql(
            "SELECT s.*
        FROM raw_station_geo s
        JOIN boundary b
        ON ST_DWithin(b.geom::geography, s.geog, {buffer_m})
        WHERE b.name = {shape_name}",
            .con = statconn
        )
    }
    tbl(statconn, sql(query), check_from = FALSE)
}

close_matches_inside <- function(statconn, state, distance_threshold) {
    query <- glue::glue_sql(
        "
            WITH
            relevant_stations AS (SELECT s.*
            FROM raw_station_geo s
            JOIN boundary b
            ON ST_DWithin(b.geom::geography, s.geog, {distance_threshold})
            WHERE b.name = {state}
            )
            SELECT a.id AS id_x, b.id AS id_y, ST_Distance(a.geog, b.geog) AS distance
            FROM relevant_stations a
            JOIN relevant_stations b
            ON ST_DWithin(a.geog, b.geog, {distance_threshold})
            WHERE a.id < b.id
            ",
        .con = statconn
    )
    tbl(statconn, sql(query))
}

filter_stations_inside <- function(metadata, boundary, statconn, buffer_m = 0) {
    copy_to(statconn, metadata, name = "stats_tmp", overwrite = TRUE)

    query <- glue::glue_sql(
        "
        SELECT s.*
        FROM stats_tmp s
        INNER JOIN boundary b
        ON b.name = {boundary} AND ST_DWithin(ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography, b.geom::geography, {buffer_m})
        ",
        .con = statconn
    )
    matches <- dbGetQuery(statconn, query)
    statconn |> dbRemoveTable("stats_tmp")
    matches
}

close_matches <- function(metadata, distance_threshold, statconn, key = "key") {
    copy_to(statconn, metadata, name = "stats_tmp", overwrite = TRUE)

    query <- glue::glue_sql(
        "
        WITH stats_tmp_geog AS (
            SELECT {`key`}, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS geog
            FROM stats_tmp
        )

        SELECT a.{`key`} AS key_x, b.{`key`} AS key_y, ST_Distance(a.geog, b.geog) AS distance
        FROM stats_tmp_geog a
        JOIN stats_tmp_geog b
        ON ST_DWithin(a.geog, b.geog, {distance_threshold}) AND a.{`key`} < b.{`key`}
        ",
        .con = statconn
    )
    matches <- dbGetQuery(statconn, query)
    statconn |> dbRemoveTable("stats_tmp")
    matches
}
