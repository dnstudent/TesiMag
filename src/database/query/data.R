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

valid_data <- function(dataconn) {
    suppressMessages(tbl(dataconn, "read_parquet('db/data/qc1/valid=true/**/*.parquet')")) |>
        select(!c(valid, starts_with("qc_")))
}

valid_series <- function(valid_data) {
    valid_data |> distinct(station_id, variable)
}

series_matches <- function(valid_series, station_matches, asymmetric = TRUE, cmp = \(x, y) x < y) {
    station_matches |>
        cross_join(tibble(variable = c(-1L, 1L)), copy = TRUE) |>
        semi_join(valid_series, by = c("id_x" = "station_id", "variable")) |>
        semi_join(valid_series, by = c("id_y" = "station_id", "variable")) |>
        filter((cmp(id_x, id_y) & asymmetric) | !asymmetric)
}
