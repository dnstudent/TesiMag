library(dplyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/database/test.R")
source("src/analysis/data/quality_check.R")
source("src/analysis/data/clim_availability.R")
source("src/pairing/matching.R")
source("src/pairing/analysis.R")
source("src/pairing/displaying.R")
source("src/pairing/combining.R")
source("notebooks/integrazioni_regionali/procedure/checkpoint.R")
source("notebooks/integrazioni_regionali/procedure/plots.R")
source("notebooks/integrazioni_regionali/procedure/tools.R")

#' Keeps only the data relevant to the specified time period. Filters out stations that do not have data in the specified time period.
#' Splits base station metadata and extra station metadata.
#'
#' @param data_pack A list containing the data and metadata loaded from .
#' @param .start The start date of the period to be kept (inclusive).
#' @param .end The end date of the period to be kept (inclusive).
#'
#' @return A database containing only the data relevant to the specified time period and the extra metadata table.
prepare_daily_data <- function(data_pack, .start, .end) {
    data_pack$data <- filter(data_pack$data, .start <= date & date <= .end) |>
        arrange(station_id, variable, date) |>
        as_arrow_table2(data_schema)

    c(base_metadata, extra_metadata) %<-% split_station_metadata(semi_join(data_pack$meta, data_pack$data, by = "station_id"))

    list("database" = as_database(base_metadata, data_pack$data) |> assert_data_uniqueness() |> assert_metadata_uniqueness(), "extra_meta" = extra_metadata)
}

#' Performs quality checks on the data and returns a list containing the data and the metadata.
#'
#' @param database The database to be checked in standard database format.
#' @param minimum_exc The minimum temperature excursions to be considered. The default value was chosen based on SCIA's data. It could be improved.
#' @param maximum_exc The maximum temperature excursions to be considered. The default value was chosen based on SCIA's data. It could be improved.
#'
#' @return A database containing the quality checked data and the remaining stations metadata in standard database format.
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
        arrange(station_id, variable, date) |>
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

    as_database(qc_stations, qc_data)
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
            aes(color = global_availability, shape = dataset_id)
        )
    list("plot" = p, "data" = spatav)
}


perform_analysis_common_ <- function(candidate_matches, database, first_date, last_date, output_path, ...) {
    analysis <- analyze_matches.hmm(candidate_matches, database, first_date, last_date)
    write_xslx_analysis(analysis, file.path("notebooks", "integrazioni_regionali", output_path), ...)
    data_table <- filter_widen_data(database, candidate_matches, first_date, last_date)
    list("analysis" = analysis, "data_table" = data_table, "full_database" = database)
}

#' Performs the analysis of the station matches between two databases.
#'
#' @param database.x The first database.
#' @param database.y The second database.
#' @param dist_km The maximum distance between stations to be considered in kilometers.
#' @param first_date The first date to be considered (inclusive).
#' @param last_date The last date to be considered (inclusive).
#' @param section The section in which the analysis table will be written (internal).
#' @param match_taggers A list of functions which will be used to tag the matches. Must include at least the tag_same_station and tag_unusable functions.
#' @param same_table Whether the two databases are the same (i.e. the same stations are present in both).
#' @param ... Additional arguments to be passed to write_xslx_analysis.
#'
#' @return A list containing the analysis table, the data table and the full database (with the data of the two databases simply concatenated)
perform_analysis <- function(database.x, database.y, dist_km, first_date, last_date, section, ...) {
    candidate_matches <- match_list(database.x$meta, database.y$meta, dist_km)
    database <- concat_databases(database.x, database.y)
    cat("Data prepared. Launching analysis...")
    perform_analysis_common_(candidate_matches, database, first_date, last_date, file.path(section, "analysis.xlsx"), ...)
    # analysis <- analyze_matches.hmm(candidate_matches, database, first_date, last_date)
    # write_xslx_analysis(analysis, file.path("notebooks", "integrazioni_regionali", section, "analysis.xlsx"), ...)
    # data_table <- filter_widen_data(database, candidate_matches, first_date, last_date)
    # list("analysis" = analysis, "data_table" = data_table, "full_database" = database)
}


perform_analysis_single <- function(database, dist_km, first_date, last_date, analysis_file, ...) {
    candidate_matches <- match_list_single(database$meta, dist_km)
    cat("Data prepared. Launching analysis...")
    perform_analysis_common_(candidate_matches, database, first_date, last_date, analysis_file, ...)
    # analysis <- analyze_matches.hmm(candidate_matches, database, first_date, last_date)
    # write_xslx_analysis(analysis, file.path("notebooks", "integrazioni_regionali", analysis_file), ...)
    # data_table <- filter_widen_data(database, candidate_matches, first_date, last_date)
    # list("analysis" = analysis, "data_table" = data_table, "full_database" = database)
}

tag_analysis <- function(analysis_results, match_taggers) {
    analysis_results$analysis <- analysis_results$analysis |>
        match_taggers$same_station() |>
        match_taggers$unusable()
    analysis_results
}

#' Build a combined database
#'
#' This function takes the analysis results and combines them into a single database.
#'
#' @param analysis_results The analysis results to be combined.
#' @param use_corrections Whether it performs corrections on the .y series when merging.
#' @param checks A logical value indicating whether to perform additional checks during the combination process.
#' @param test_bounds Test that corrections (in absolute value) are less than this.
#' @param match_selector A function that takes a match list and returns a subset of it. Its intended use it to exclude duplicate matches.
#' @param ... Additional arguments to be passed to merge_database_by for match prioritization.
#' @return The combined database and the (filtered) match list that produced it.
build_combined_database <- function(analysis_results, use_corrections, checks, test_bounds, match_selectors, ...) {
    match_list <- analysis_results$analysis |> filter(same_station & !unusable)
    merged_data <- merge_database_by(match_list, analysis_results$data_table, .test_bounds = test_bounds, match_selectors, use_corrections, ...) |> as_arrow_table2(data_schema)
    combined_database <- concat_merged_and_unmerged(
        merged_data,
        analysis_results$full_database,
        match_list,
        checks
    )
    list("database" = combined_database, "match_list" = match_list)
}
