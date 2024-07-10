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


#' WARNING: When an error with the message "Could not find table 'x'" is raised, it is likely that there is an error in the query.
#' Check again e.g. the join columns
query_parquet <- function(path, conn = NULL, filename = FALSE) {
    if (length(path) > 1L) path <- paste0(path, collapse = "', '")
    if (is.null(conn)) {
        conn <- dbConnect(duckdb())
        dbExecute(conn, "INSTALL icu; LOAD icu;")
    }
    tbl_query <- str_glue("read_parquet(['{path}'], filename = {filename})")
    tbl(conn, tbl_query)
}

query_dataset <- function(path, conn = NULL, filename = FALSE, hive_types = list()) {
    path <- file.path(path, "**", "*.parquet")
    if (length(path) > 1L) path <- paste0(path, collapse = "', '")
    if (is.null(conn)) {
        conn <- dbConnect(duckdb())
        dbExecute(conn, "INSTALL icu; LOAD icu;")
    }
    if (length(hive_types) > 0L) {
        hive_types <- purrr::map2_chr(names(hive_types), hive_types, ~ str_glue("'{.x}': {.y}")) |> paste(collapse = ", ")
        hive_types <- str_glue(", hive_types = {{ {hive_types} }}")
    } else {
        hive_types <- ""
    }
    tbl_query <- str_glue("read_parquet(['{path}'], filename = {filename}{hive_types})")
    tbl(conn, tbl_query)
}

query_checkpoint_data <- function(datasets, step, conn = NULL, filename = FALSE, hive_types = list("variable" = "INT")) {
    query_dataset(archive_path(datasets, "data", step), conn, filename, hive_types)
}

query_checkpoint_meta <- function(datasets, step = "raw", conn = NULL, filename = FALSE, hive_types = list()) {
    query_dataset(archive_path(datasets, "metadata", step), conn, filename, hive_types)
}

query_checkpoint <- function(datasets, step, conn = NULL, all_stations = FALSE, filename = FALSE) {
    if (is.null(conn)) {
        conn <- dbConnect(duckdb())
        dbExecute(conn, "INSTALL icu; LOAD icu;")
    }
    meta_step <- if (all_stations) "raw" else step
    list(
        "meta" = query_checkpoint_meta(datasets, meta_step, conn, filename),
        "data" = query_checkpoint_data(datasets, step, conn, filename)
    )
}

valid_series <- function(valid_data) {
    valid_data |> distinct(key, variable)
}

useful_data <- function(data_query) {
    stations <- tbl(data_query$src$con, "raw_stations_tmp") |>
        filter(lat > 42.1, !(geom_district %in% c("Lazio", "Abruzzo"))) |>
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
