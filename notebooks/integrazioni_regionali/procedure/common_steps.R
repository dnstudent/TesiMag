library(dplyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/analysis/data/quality_check.R")
source("src/analysis/data/clim_availability.R")
source("src/pairing/matching.R")
source("src/pairing/analysis.R")
source("notebooks/integrazioni_regionali/procedure/checkpoint.R")
source("notebooks/integrazioni_regionali/procedure/plots.R")
source("notebooks/integrazioni_regionali/procedure/tools.R")


#' Keeps only the data relevant to the specified time period. Filters out stations that do not have data in the specified time period.
#'
#' @param full_station_list A table of stations to be filtered.
#' @param daily_data_long A table of the data to be filtered in standard data format. full_station_list and daily_data_long must be of the same class.
#' @param .start The start date of the period to be kept (inclusive).
#' @param .end The end date of the period to be kept (inclusive).
prepare_daily_data <- function(database, .start, .end, dem) {
    database$data <- filter(database$data, .start <= date & date <= .end) |>
        arrange(station_id, variable, date) |>
        as_arrow_table2(data_schema)
    database$meta <- semi_join(database$meta, database$data, by = "station_id") |> compute()

    database
}

#' Performs quality checks on the data and returns a list containing the data and the metadata.
#'
#' @param full_station_list An Arrow Table containing the list of stations to be checked.
#' @param daily_data_long An Arrow Table containing the data to be checked in standard data format.
#' @param minimum_exc The minimum temperature excursions to be considered. The default value was chosen based on SCIA's data. It could be improved.
#' @param maximum_exc The maximum temperature excursions to be considered. The default value was chosen based on SCIA's data. It could be improved.
#'
#' @return A list containing the quality checked data and the remaining stations metadata in standard data format.
qc1 <- function(database, minimum_exc = 0.05, maximum_exc = 50, stop_on_error = TRUE) {
    database$data <- database$data |> compute()

    database$data |>
        group_by(station_id, variable, date) |>
        tally() |>
        collect() |>
        verify(n == 1L)

    database$meta |>
        assert(is_uniq, station_id)

    original_length <- nrow(database$data)

    qc_data <- database$data |>
        gross_errors_check(value) |>
        group_by(station_id, variable) |>
        collect() |>
        repeated_values_check() |>
        integer_streak_check(threshold = 8L) |>
        filter(!(qc_gross | qc_repeated | qc_int_streak)) |>
        select(!starts_with("qc_")) |>
        pivot_wider(id_cols = c(station_id, date), names_from = variable, values_from = value) |>
        filter(minimum_exc < (T_MAX - T_MIN) & (T_MAX - T_MIN) < maximum_exc) |>
        pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        mutate(merged = FALSE) |>
        as_arrow_table2(data_schema)

    if ((nrow(qc_data) / original_length) < 0.9) {
        if (stop_on_error) {
            fn <- stop
        } else {
            fn <- warn
        }
        fn("The resulting dataset has an unusually small number of elements wrt to the original")
    }

    qc_stations <- database$meta |>
        semi_join(qc_data, by = "station_id", relationship = "one-to-many") |>
        assert(is_uniq, station_id) |>
        as_arrow_table2(station_schema)

    list("meta" = qc_stations, "data" = qc_data)
}

#' Produces the plot of year-monthly series availabilities (the number of available and usable series per year/month) and the table used to compute them.
#'
#' @param station_list An Arrow Table containing the list of stations to be considered.
#' @param daily_data_long An Arrow Table containing the data to be considered in standard data format.
#' @param against An optional list containing either:
#'  - the name of the dataset to be compared against (must have been processed with the standard approach);
#'  - the stations and data to be compared against;
#'  - NULL, in which case the function will only plot the availability of the specified data.
#' @param inside An optional sf object containing the boundaries of the area to be considered.
#' @param start_date The start date of the period to be considered (inclusive).
#' @param end_date The end date of the period to be considered (inclusive).
#' @param ... Additional arguments to be passed to is_month_usable.
#'
#' @return A list containing the plot and the data.
ymonthly_availabilities <- function(database, against = NULL, region = NULL, chkp_id = "last", start_date = NULL, end_date = NULL, ...) {
    if (is.character(against)) {
        against <- open_checkpoint(against, chkp_id) |> filter_checkpoint_inside(region)
    } else if (is.null(against)) {
        return(
            plot_state_avail(
                database$meta,
                database$data,
                start_date,
                end_date
            )
        )
    }
    full_db <- concat_databases(database, against)
    plot_state_avail(
        full_db$meta,
        full_db$data,
        start_date,
        end_date
    )
}

#' Produces the table and plot of spatial series availabilities.
spatial_availabilities <- function(ymonthly_avail, stations, map, ...) {
    spatav <- ymonthly_avail |>
        is_climatology_computable(available, .start = start_date, .end = end_date, monthly_usabilities = TRUE, ...) |>
        ungroup()
    p <- ggplot() +
        geom_sf(data = map) +
        geom_sf(
            data = spatav |>
                group_by_key() |>
                summarise(global_availability = all(clim_available)) |>
                left_join(stations |> select(station_id, lon, lat) |> collect(), by = "station_id") |>
                st_md_to_sf(),
            aes(color = global_availability)
        )
    list("plot" = p, "data" = spatav)
}

build_combined_database <- function(database.x, database.y, dist_km, first_date, last_date, asymmetric = FALSE) {
    database <- concat_databases(database.x, database.y)

    candidate_matches <- match_list(database.x$meta, database.y$meta, dist_km, asymmetric)
    data_table <- filter_widen_data(database, candidate_matches, first_date, last_date)

    analysis <- analyze_matches(data_table, candidate_matches, database$meta)
    list(candidate_matches, data_table, analysis, database)
}
