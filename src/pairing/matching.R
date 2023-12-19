library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)

source("src/load/tools.R")
source("src/analysis/metadata.R")

match_list <- function(meta.x, meta.y, dist_km) {
    meta.x <- meta.x |>
        collect() |>
        st_md_to_sf(remove = FALSE)
    meta.y <- meta.y |>
        collect() |>
        st_md_to_sf(remove = FALSE)
    matches <- sf::st_join(
        meta.x,
        meta.y,
        left = FALSE,
        suffix = c(".x", ".y"),
        join = sf::st_is_within_distance,
        dist = units::set_units(dist_km, "km")
    ) |>
        sf::st_drop_geometry() |>
        select(station_id.x, station_id.y) |>
        as_arrow_table(schema = schema(station_id.x = utf8(), station_id.y = utf8()))
}

match_list_single <- function(meta, dist_km) {
    match_list(meta, meta, dist_km) |>
        filter((station_id.x != station_id.y)) |> # It is still not completely filtered! Next step should be done with valid_days
        compute()
}

widen_split_data.single <- function(data_ds, match_table, which_identifier, first_date, last_date) {
    # stations <- match_table |>
    #     select(station_id = all_of(paste0("station_id.", which)))
    # series_info <- select(series_table, series_id, station_id) |> semi_join(stations, by = "station_id")
    semi_join(data_ds |> filter(as.Date(first_date) <= date, date <= as.Date(last_date)),
        match_table,
        by = join_by(series_id == {{ which_identifier }}),
        relationship = "many-to-one"
    ) |>
        collect() |>
        pivot_wider(id_cols = date, names_from = series_id, values_from = value) |>
        arrange(date) |>
        as_tsibble(index = date) |>
        fill_gaps(.start = first_date, .end = last_date)
    # arrange(variable) |>
    # group_by(variable) |>
    # group_map(
    # )
}

widen_split_data <- function(ds.x, ds.y, match_table, first_date, last_date) {
    list(
        widen_split_data.single(ds.x, match_table, series_id.x, first_date, last_date),
        widen_split_data.single(ds.y, match_table, series_id.y, first_date, last_date)
    )
}

filter_widen_data.old <- function(ds, match_list, first_date, last_date) {
    all_ids <- concat_tables(
        match_list |> select(series_id = series_id.x) |> compute(),
        match_list |> select(series_id = series_id.y) |> compute()
    )
    ds |>
        filter(first_date <= date & date <= last_date) |>
        semi_join(all_ids, by = "series_id", relationship = "many-to-one") |>
        collect() |>
        pivot_wider(id_cols = date, names_from = series_id, values_from = value) |>
        arrange(date) |>
        as_tsibble(index = date) |>
        fill_gaps(.start = first_date, .end = last_date)
}

filter_widen_data <- function(database, match_list, first_date, last_date) {
    station_ids <- concat_tables(
        match_list |> select(station_id = station_id.x) |> compute(),
        match_list |> select(station_id = station_id.y) |> compute(),
        unify_schemas = FALSE
    )
    database$data |>
        filter(first_date <= date & date <= last_date) |>
        semi_join(station_ids, by = "station_id", na_matches = "never") |>
        collect() |>
        pivot_wider(id_cols = date, names_from = c(variable, station_id), values_from = value, names_sep = "_") |>
        arrange(date) |>
        as_tsibble(index = date) |>
        fill_gaps(.start = first_date, .end = last_date)
}

join_data_on_matchlist <- function(table.x, match_list, table.y) {
    if ("variable" %in% colnames(match_list)) {
        match_keys <- c("station_id.x", "variable")
    } else {
        match_keys <- c("station_id.x")
    }
    match_list |>
        left_join(table.x |> rename(station_id.x = station_id), by = match_keys, relationship = "many-to-many") |>
        left_join(table.y, join_by(station_id.y == station_id, variable, date), relationship = "one-to-one")
}

bijoin_data_on_matchlist <- function(match_list, table) {
    join_data_on_matchlist(table, match_list, table)
}

bijoin_metadata_on_matchlist <- function(match_list, metadata_list) {
    match_list |>
        left_join(metadata_list, join_by(station_id.x == station_id), relationship = "one-to-one") |>
        left_join(metadata_list, join_by(station_id.y == station_id), relationship = "one-to-one")
    # right_join(metadata_list, match_list, join_by(station_id == station_id.x)) |>
    #     rename(station_id.x = station_id) |>
    #     left_join(metadata_list, join_by(station_id.y == station_id))
}
