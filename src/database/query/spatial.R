library(DBI, warn.conflicts = FALSE)
library(RPostgres, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)

query_boundary <- function(conn, name, kind) {
    st_read(conn, query = glue::glue_sql('SELECT * FROM regional_boundaries WHERE "shapeName" = {name} AND kind = {kind}', .con = conn), geometry_column = "geometry", quiet = TRUE)
}

query_boundaries <- function(conn, of) {
    copy_to(conn, of, name = "meta_bounds_tmp", overwrite = TRUE)
}

query_district_matches <- function(statconn, district_name, distance_threshold, buffer_m, cmp = "<") {
    query <- glue::glue(
        "
        WITH inside_stations AS (SELECT s.*
        FROM raw_station_geo s
        JOIN regional_boundaries b
        ON ST_DWithin(b.geom::geography, s.geog, {buffer_m})
        WHERE b.name = '{district_name}'
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

close_matches_inside <- function(statconn, district, distance_threshold) {
    query <- glue::glue_sql(
        "
            WITH
            relevant_stations AS (SELECT s.*
            FROM raw_station_geo s
            JOIN boundary b
            ON ST_DWithin(b.geom::geography, s.geog, {distance_threshold})
            WHERE b.name = {district}
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
    copy_to(statconn, metadata |> select(key, lon, lat), name = "stats_tmp", overwrite = TRUE)

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

#' Data una tabella di metadati
close_matches.bruno <- function(metadata, distance_threshold) {
    georef <- metadata |>
        select(key, lon, lat) |>
        st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = F)

    key_pairs <- georef |>
        st_join(georef, join = st_is_within_distance, dist = units::set_units(distance_threshold, "m"), suffix = c("_x", "_y")) |>
        st_drop_geometry() |>
        filter(key_x < key_y)

    sol <- key_pairs |>
        mutate(
            distance = st_distance(
                st_as_sf(pick(lon_x, lat_x), crs = "EPSG:4326", coords = c("lon_x", "lat_x")),
                st_as_sf(pick(lon_y, lat_y), crs = "EPSG:4326", coords = c("lon_y", "lat_y")),
                by_element = TRUE
            ) |> as.numeric()
        ) |>
        select(key_x, key_y, distance)
}

query_elevations <- function(metadata, statconn) {
    copy_to(statconn, metadata |> select(dataset, sensor_key, lon, lat), name = "stats_tmp", overwrite = TRUE)
    query <- glue::glue_sql(
        "
        WITH s AS (
            SELECT dataset, sensor_key, ST_SetSRID(ST_MakePoint(lon, lat), 4326) AS geom
            FROM stats_tmp
        )
        SELECT s.dataset, s.sensor_key, AVG(ST_Value(r.rast, 1, s.geom, true, 'bilinear')) AS elevation_glo30
        FROM s
        INNER JOIN copernicus_glo30 r
        ON r.rast && s.geom
        GROUP BY s.dataset, s.sensor_key
        ",
        .con = statconn
    )
    melev <- dbGetQuery(statconn, query)
    statconn |> dbRemoveTable("stats_tmp")
    metadata |>
        left_join(melev, by = c("sensor_key", "dataset"), relationship = "one-to-one") |>
        mutate(elevation_glo30 = coalesce(elevation_glo30, 0))
    # distinct(dataset, sensor_key, .keep_all = TRUE)
}

closest_within <- function(x, y, distance_threshold, statconn) {
    copy_to(statconn, x, name = "x_tmp", overwrite = TRUE)
    copy_to(statconn, y, name = "y_tmp", overwrite = TRUE)
    query <- glue::glue_sql(
        "
        WITH x_geog AS (
            SELECT sensor_key, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS geog
            FROM x_tmp
        ),
        y_geog AS (
            SELECT user_code, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS geog
            FROM y_tmp
        )
        SELECT sensor_key, user_code, ST_Distance(x.geog, y.geog) AS distance
        FROM x_geog x
        JOIN y_geog y
        ON ST_DWithin(x.geog, y.geog, {distance_threshold})
        ",
        .con = statconn
    )
    matches <- dbGetQuery(statconn, query)
    statconn |>
        dbRemoveTable("x_tmp")
    statconn |>
        dbRemoveTable("y_tmp")
    matches |> as_tibble()
}

query_distance <- function(pairs, statconn) {
    copy_to(statconn, pairs, name = "pairs_tmp", overwrite = TRUE)
    query <- glue::glue_sql(
        "
        WITH pairs_geog AS (
            SELECT key_x, key_y, ST_SetSRID(ST_MakePoint(lon_x, lat_x), 4326)::geography AS geog_x, ST_SetSRID(ST_MakePoint(lon_y, lat_y), 4326)::geography AS geog_y
            FROM pairs_tmp
        )
        SELECT key_x, key_y, ST_Distance(geog_x, geog_y) AS distance
        FROM pairs_geog
        ",
        .con = statconn
    )
    matches <- dbGetQuery(statconn, query)
    statconn |> dbRemoveTable("pairs_tmp")
    matches
}
