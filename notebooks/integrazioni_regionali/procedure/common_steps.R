library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(assertthat, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/database/test.R")
source("src/database/data_model.R")
source("src/analysis/data/quality_check.R")
source("src/merging/combining.R")
source("notebooks/integrazioni_regionali/procedure/checkpoint.R")
source("notebooks/integrazioni_regionali/procedure/plots.R")
source("notebooks/integrazioni_regionali/procedure/tools.R")

test_metadata_consistency <- function(meta) {
    meta |> verify(!is.na(series_id))

    meta |>
        group_by(sensor_id, station_id, series_id) |>
        count() |>
        verify(n == 1L)

    assert_that(meta |>
        filter(if_all(c(sensor_id, station_id, series_id), is.na)) |>
        nrow() == 0L)

    assert_that(meta |>
        filter(if_any(c(dataset, network, lon, lat, kind), is.na)) |>
        nrow() == 0L)

    meta
}

test_data_consistency <- function(data) {
    if (data |>
        filter(if_any(c(dataset, sensor_key, variable, date), is.na)) |>
        compute() |>
        nrow() > 0L) {
        stop("Data contains NA keys")
    }

    if (data |> group_by(dataset, sensor_key, variable, date) |> count() |> filter(n > 1L) |> compute() |> nrow() > 0L) {
        stop("Data contains duplicate measures for the same key")
    }

    data
}

make_keys <- function(meta) {
    meta |>
        collect() |>
        mutate(
            sensor_key = row_number(),
            dummy_station_id = coalesce(station_id, series_id)
        ) |>
        group_by(dummy_station_id) |>
        mutate(station_key = cur_group_id()) |>
        ungroup() |>
        select(-dummy_station_id) |>
        group_by(series_id) |>
        mutate(series_key = cur_group_id()) |>
        ungroup()
}

#' Given a measures table and a metadata table, associates the id given in the metadata table to the measures table's key
#' and returns the resulting table.
associate_sensor_key <- function(data, meta) {
    common_id <- intersect(names(data |> select(ends_with("_id"), any_of("user_code"))), names(meta |> select(ends_with("_id"), user_code)))
    data |>
        left_join(
            meta |> mutate(sensor_first = coalesce(sensor_first, station_first, series_first), sensor_last = coalesce(sensor_last, station_last, series_last)) |> select(all_of(common_id), sensor_key, sensor_first, sensor_last),
            by = common_id,
            relationship = "many-to-many"
        ) |>
        filter((is.na(sensor_first) | sensor_first <= date) & (date <= sensor_last | is.na(sensor_last))) |>
        select(!c(all_of(common_id), sensor_first, sensor_last))
}

date_meta <- function(data, meta) {
    data |>
        left_join(meta |> select(dataset, sensor_key, station_key, series_key), by = c("dataset", "sensor_key"), relationship = "many-to-one") |>
        group_by(dataset, sensor_key, station_key, series_key) |>
        summarise(
            sensor_first = min(date, na.rm = FALSE),
            sensor_last = max(date, na.rm = FALSE),
        ) |>
        collect() |>
        group_by(dataset, station_key) |>
        mutate(
            station_first = min(sensor_first, na.rm = FALSE),
            station_last = max(sensor_last, na.rm = FALSE),
        ) |>
        group_by(dataset, series_key) |>
        mutate(
            series_first = min(sensor_first, na.rm = FALSE),
            series_last = max(sensor_last, na.rm = FALSE),
        ) |>
        ungroup()
}

prepare_daily_data <- function(data_pack, statconn = NULL) {
    data_pack$meta <- make_keys(data_pack$meta) |>
        associate_regional_info(statconn) |>
        mutate(sensor_first = coalesce(sensor_first, station_first), sensor_last = coalesce(sensor_last, station_last)) |>
        test_metadata_consistency() |>
        as_arrow_table()

    data_pack$data <- data_pack$data |>
        filter(!is.na(value)) |>
        mutate(variable = if_else(variable == "T_MIN", -1L, 1L)) |>
        associate_sensor_key(data_pack$meta) |>
        test_data_consistency() |>
        arrange(sensor_key, variable, date) |>
        compute()

    date_metas <- date_meta(data_pack$data, data_pack$meta) |>
        as_arrow_table()

    meta <- data_pack$meta |>
        select(!c(ends_with("_first"), ends_with("_last"))) |>
        left_join(date_metas, by = c("dataset", "sensor_key", "station_key", "series_key")) |>
        select(all_of(names(meta_schema))) |>
        compute()
    extra_meta <- data_pack$meta |>
        select(-names(meta_schema), sensor_key) |>
        compute()

    # split <- split_station_metadata(data_pack$meta)
    list("checkpoint" = as_checkpoint(meta, data_pack$data), "extra_meta" = extra_meta)
}

qc_checkpoint <- function(dataset, conn) {
    ds <- query_checkpoint(dataset, "raw", conn)
    qc_data <- qc1(ds$data)
    qc_meta <- ds$meta |> semi_join(filter(qc_data, valid), by = c("dataset", "sensor_key"))
    as_checkpoint(meta = qc_meta |> to_arrow(), data = qc_data |> to_arrow(), check_schema = FALSE) |> save_checkpoint(dataset, "qc1", check_schema = FALSE)
}

#' Produces the plot of year-monthly series availabilities (the number of available and usable series per year/month) and the table used to compute them.
#' The plot is faceted by variable.
#'
#' @param data The data to be used in standard data format.
#' @param ... Additional arguments to be passed to monthly_availabilities (the availability thresholds).
#'
#' @return A list containing the plot and the data.
ymonthly_availabilities <- function(data, ...) {
    plot_state_avail.tbl(
        data, ...
    )
}

#' Produces the table and plot of spatial series availabilities.
spatial_availabilities <- function(ymonthly_avail, stations, map, ...) {
    spatav <- clim_availability(ymonthly_avail, ...)
    # collect()

    p <- ggplot() +
        geom_sf(data = map) +
        geom_sf(
            data = spatav |>
                inner_join(stations |> select(dataset, sensor_key, lon, lat), join_by(dataset, sensor_key)) |>
                collect() |>
                st_md_to_sf(),
            aes(color = qc_clim_available, shape = dataset)
        )
    list("plot" = p, "data" = spatav)
}

merge_same_series <- function(tagged_analysis, metadata, data, ...) {
    gs <- series_groups(tagged_analysis, metadata, data, tag_same_series, group_by_component, FALSE)
    ranked_series_groups <- gs$table |>
        rank_series_groups(metadata, ...)
    merged <- dynamic_merge(data, ranked_series_groups, tagged_analysis)
    list("series_groups" = ranked_series_groups, "data" = merged, "graph" = gs$graph)
}
