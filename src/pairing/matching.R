library(sf, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)


source("src/analysis/metadata.R")


match_table <- function(meta.x, meta.y, dist_km, asymmetric = FALSE) {
    meta.x <- meta.x |>
        collect() |>
        st_md_to_sf()
    meta.y <- meta.y |>
        collect() |>
        st_md_to_sf()
    matches <- st_join(
        meta.x,
        meta.y,
        left = FALSE,
        suffix = c(".x", ".y"),
        join = st_is_within_distance,
        dist = units::set_units(dist_km, "km")
    ) |>
        st_drop_geometry() |>
        filter(variable.x == variable.y) |>
        select(-variable.x) |>
        rename(variable = variable.y) |>
        mutate(match_id = as.character(row_number()))
    if (asymmetric) {
        matches <- matches |>
            filter((last_date.x >= last_date.y) & (identifier.x != identifier.y))
    }
    matches |>
        select(starts_with("series_id."), match_id, variable) |>
        as_arrow_table()
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

three_way_join <- function(table.x, match_table, table.y) {
    inner_join(
        table.x |> rename(series_id.x = series_id) |> select(-variable),
        table.y |> rename(series_id.y = series_id) |> left_join(
            match_table |> select(starts_with("series_id."), match_id),
            by = "series_id.y"
        ),
        by = "series_id.x"
    )
}
