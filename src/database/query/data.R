library(duckdb, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

source("src/database/open.R")

query_from_connection <- function(stations, data_table_name, dataconn) {
    stats_tbl <- stations |>
        select(dataset, id) |>
        as_arrow_table() |>
        to_duckdb(dataconn, "stations_tmp")

    dbGetQuery(
        dataconn,
        "SELECT d.*
        FROM ? d
        JOIN stations_tmp s
        ON d.dataset = s.dataset, d.station_id = s.id",
        list(data_table_name)
    )
}

#' Dplyr semi_join where the left table is a duckdb table and the tight table is not.
#'
#' @param x A duckdb table
#' @param y An object coercible to an arrow table
#' @param ... Additional arguments passed to semi_join
#' @param .table_name The name of the temporary table to create
#'
#' @return The semi_join of x and y
semi_join.ddb <- function(x, y, ...) {
    y_tbl <- y |>
        as_arrow_table() |>
        to_duckdb(con = x$src$con, table_name = "station_tmp", auto_disconnect = FALSE)

    x |> semi_join(y_tbl, ...)
}

#' WARNING: When an error with the message "Could not find table 'x'" is raised, it is likely that there is an error in the query.
#' Check again e.g. the join columns
query_parquet <- function(path, conn = NULL, filename = FALSE) {
    if (length(path) > 1L) path <- paste0(path, collapse = "', '")
    if (is.null(conn)) {
        conn <- dbConnect(duckdb())
        dbExecute(conn, "INSTALL icu; LOAD icu;")
    }
    tbl_query <- str_glue("read_parquet(['{path}'], filename = {filename})")
    suppressMessages(tbl(conn, tbl_query))
}

query_checkpoint_data <- function(datasets, step, conn = NULL, filename = FALSE) {
    query_parquet(archive_path(datasets, "data", step), conn, filename)
}

query_checkpoint_meta <- function(datasets, step = "raw", conn = NULL, filename = FALSE) {
    query_parquet(archive_path(datasets, "metadata", step), conn, filename)
}

query_checkpoint <- function(datasets, step, conn = NULL, all_stations = TRUE, filename = FALSE) {
    meta_step <- if (all_stations) "raw" else step
    list(
        "meta" = query_checkpoint_meta(datasets, meta_step, conn, filename),
        "data" = query_checkpoint_data(datasets, step, conn, filename)
    )
}

valid_data <- function(dataconn) {
    suppressMessages(tbl(dataconn, "read_parquet('db/data/qc1/valid=true/**/*.parquet', hive_partitioning = 1, hive_types = {'variable': int, 'dataset': text})")) |>
        filter(valid) |>
        select(!c(valid, starts_with("qc_")))
}

valid_series <- function(valid_data) {
    valid_data |> distinct(key, variable)
}

useful_data <- function(data_query) {
    stations <- tbl(data_query$src$con, "raw_stations_tmp") |>
        filter(lat > 42.1, !(geom_state %in% c("Lazio", "Abruzzo"))) |>
        select(id)

    data_query |>
        filter(date >= as.Date("1990-01-01")) |>
        semi_join(stations, by = c("station_id" = "id"))
}

series_matches <- function(data, station_matches, metadata) {
    valid_series <- data |>
        semi_join(metadata, by = "key") |>
        distinct(key, variable) |>
        collect()

    station_matches |>
        cross_join(tibble(variable = c(-1L, 1L))) |>
        semi_join(valid_series, by = c("key_x" = "key", "variable")) |>
        semi_join(valid_series, by = c("key_y" = "key", "variable"))
}
