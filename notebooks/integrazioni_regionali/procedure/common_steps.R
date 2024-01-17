library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/database/test.R")
source("src/database/definitions.R")
source("notebooks/integrazioni_regionali/procedure/checkpoint.R")
source("notebooks/integrazioni_regionali/procedure/plots.R")
source("notebooks/integrazioni_regionali/procedure/tools.R")

new_numeric_ids <- function(data_pack, new_dataset) {
    data_pack$meta <- data_pack$meta |>
        collect() |>
        mutate(previous_id = as.character(id), id = row_number(), previous_dataset = dataset, dataset = new_dataset) |>
        as_arrow_table()
    data_pack$data <- data_pack$data |>
        mutate(station_id = as.character(station_id)) |>
        rename(previous_id = station_id, previous_dataset = dataset) |>
        left_join(data_pack$meta |> select(station_id = id, previous_id, dataset, previous_dataset), by = c("previous_id")) |>
        as_arrow_table2(data_schema)
    data_pack
}

#' Keeps only the data relevant to the specified time period. Filters out stations that do not have data in the specified time period.
#' Splits base station metadata and extra station metadata.
#'
#' @param data_pack A list containing the data and metadata loaded from .
#' @param .start The start date of the period to be kept (inclusive).
#' @param .end The end date of the period to be kept (inclusive).
#'
#' @return A database containing only the data relevant to the specified time period and the extra metadata table.
prepare_daily_data <- function(data_pack, dataset_name) {
    # data_pack <- new_numeric_ids(data_pack, dataset_name)
    data_pack$data <- data_pack$data |>
        filter(!is.na(value)) |>
        arrange(station_id, variable, date) |>
        compute()

    date_stats <- data_pack$data |>
        group_by(station_id) |>
        summarize(
            first_registration = min(date),
            last_registration = max(date),
            valid_days = as.integer(sum(!is.na(value)) / 2L),
            .groups = "drop"
        )

    data_pack$meta <- data_pack$meta |>
        semi_join(data_pack$data, join_by(original_id == station_id)) |>
        left_join(date_stats, join_by(original_id == station_id), relationship = "one-to-one") |>
        compute()

    split <- split_station_metadata(data_pack$meta)
    list("database" = as_database(split[[1]], data_pack$data) |> assert_data_uniqueness() |> assert_metadata_uniqueness(), "extra_meta" = split[[2]])
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
    spatav <- clim_availability(ymonthly_avail, ...) |>
        collect()

    p <- ggplot() +
        geom_sf(data = map) +
        geom_sf(
            data = spatav |>
                left_join(stations |> select(dataset, id, lon, lat) |> collect(), join_by(dataset, station_id == id)) |>
                st_md_to_sf(),
            aes(color = qc_clim_available, shape = dataset)
        )
    list("plot" = p, "data" = spatav)
}
