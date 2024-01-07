library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/database/test.R")
source("src/database/definitions.R")
source("src/analysis/data/quality_check.R")
source("src/analysis/data/clim_availability.R")
source("src/pairing/matching.R")
source("src/pairing/analysis.R")
source("src/pairing/displaying.R")
source("src/pairing/combining.R")
source("src/pairing/tools.R")
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
    data_pack <- new_numeric_ids(data_pack, dataset_name)

    data_pack$data <- data_pack$data |>
        filter(!is.na(value)) |>
        arrange(dataset, station_id, variable, date) |>
        compute()

    date_stats <- data_pack$data |>
        group_by(dataset, station_id) |>
        summarize(
            first_registration = min(date),
            last_registration = max(date),
            valid_days = as.integer(sum(!is.na(value)) / 2L),
            .groups = "drop"
        )

    data_pack$meta <- data_pack$meta |>
        semi_join(data_pack$data, join_by(dataset, id == station_id)) |>
        left_join(date_stats, join_by(dataset, id == station_id), relationship = "one-to-one") |>
        compute()

    c(base_metadata, extra_metadata) %<-% split_station_metadata(data_pack$meta)

    list("database" = as_database(base_metadata, data_pack$data) |> assert_data_uniqueness() |> assert_metadata_uniqueness(), "extra_meta" = extra_metadata)
}

#' Performs quality checks on the data and returns a list containing the data and the metadata.
#'
#' @param database The database to be checked in standard database format.
#' @param minimum_exc The minimum temperature excursions to be considered. The default value was chosen based on SCIA's data. It could be improved.
#' @param maximum_exc The maximum temperature excursions to be considered. The default value was chosen based on SCIA's data. It could be improved.
#'
#' @return A database containing the quality checked data and the remaining stations metadata in standard database format.
# qc1 <- function(database, minimum_exc = 0.05, maximum_exc = 50, stop_on_error = TRUE) {
#     database$data <- database$data |> to_duckdb()
#
#     database$data |>
#         group_by(dataset, station_id, variable, date) |>
#         tally() |>
#         collect() |>
#         verify(n == 1L)
#
#     database$meta |>
#         collect() |>
#         assert(is_uniq, id)
#
#     original_length <- nrow(database$data)
#
#     qc_data <- database$data |>
#         arrange(dataset, station_id, variable, date) |>
#         gross_errors_check(value) |>
#         group_by(dataset, station_id, variable) |>
#         # collect() |>
#         repeated_values_check() |>
#         integer_streak_check(threshold = 8L) |>
#         filter(!(qc_gross | qc_repeated | qc_int_streak)) |>
#         select(!starts_with("qc_")) |>
#         pivot_wider(id_cols = c(dataset, station_id, date), names_from = variable, values_from = value) |>
#         filter(minimum_exc < (T_MAX - T_MIN) & (T_MAX - T_MIN) < maximum_exc) |>
#         pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
#         to_arrow() |>
#         as_arrow_table2(data_schema)
#
#     if ((nrow(qc_data) / original_length) < 0.9) {
#         if (stop_on_error) {
#             fn <- stop
#         } else {
#             fn <- warn
#         }
#         fn("The resulting dataset has an unusually small number of elements wrt to the original")
#     }
#
#     qc_stations <- database$meta |>
#         semi_join(qc_data, join_by(dataset, id == station_id), relationship = "one-to-many") |>
#         as_arrow_table2(station_schema)
#
#     as_database(qc_stations, qc_data)
# }

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


perform_analysis_common_ <- function(analysis, candidate_matches, database, first_date, last_date, output_path, ...) {
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
    analysis <- analyze_matches(candidate_matches, database, first_date, last_date, symmetric = FALSE, checks = TRUE)
    perform_analysis_common_(analysis, candidate_matches, database, first_date, last_date, file.path(section, "analysis.xlsx"), ...)
}


perform_analysis_symmetric <- function(database, dist_km, first_date, last_date, analysis_file, filter_symmetric, priority_table, ...) {
    candidate_matches <- match_list_single(database$meta, dist_km, priority_table)
    cat("Data prepared. Launching analysis...")
    analysis <- analyze_matches(candidate_matches, database, first_date, last_date, symmetric = TRUE, checks = TRUE)
    if (!is.null(filter_symmetric)) {
        # Check
        original_ids <- tibble(station_id = c(analysis$station_id.x, analysis$station_id.y) |> unique())
        analysis <- filter_symmetric(analysis)
        remaining_ids <- tibble(station_id = c(analysis$station_id.x, analysis$station_id.y) |> unique())
        eventually_left_out <- anti_join(original_ids, remaining_ids, by = "station_id")
        if (nrow(eventually_left_out) > 0) {
            print("There was a problem in the match filtering:")
            print(eventually_left_out)
            stop()
        }
        if (contains_symmetric_duplicates(analysis, "station_id.x", "station_id.y")) {
            warn("There was a problem in the symmetric duplicates filtering")
        }
    }
    perform_analysis_common_(analysis, candidate_matches, database, first_date, last_date, analysis_file, ...)
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
build_combined_database <- function(analysis_results, use_corrections, checks, test_bounds, ...) {
    match_list <- analysis_results$analysis |> filter(same_station & !unusable)
    merged_data <- merge_database_by(match_list, analysis_results$data_table, .test_bounds = test_bounds, use_corrections, ...) |> as_arrow_table2(data_schema)
    combined_database <- concat_merged_and_unmerged(
        merged_data,
        analysis_results$full_database,
        match_list,
        checks
    )
    list("database" = combined_database, "match_list" = match_list)
}
