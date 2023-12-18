library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(stringi, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)

source("src/database/write.R")

annual_index <- function(date) {
    2 * pi * yday(date) / yday(ceiling_date(date, unit = "year") - as.difftime(1, units = "days"))
}

#' Computes a diffs table for a given match table, loading the data series from the given tables.
#'
#' @param match_list A match table, as returned by \code{\link{match_table}}, complete with two series identifer columns and a match_id column.
#' @param data_table A table containing the data series for the first database, one per column, and a date index.
#' @return A tsibble with a column for each match_id, containing the difference between the two series linked to the id.
diffs_table <- function(match_list, data_table) {
    match_list |>
        select(variable, station_id.x, station_id.y, match_id) |>
        rowwise() |>
        reframe(
            diffs = data_table |> pull(paste0(variable, "_", station_id.x)) - data_table |> pull(paste0(variable, "_", station_id.y)),
            date = data_table$date,
            match_id = match_id
        ) |>
        pivot_wider(id_cols = date, values_from = diffs, names_from = match_id) |>
        arrange(date) |>
        as_tsibble(index = date) |>
        fill_gaps(.full = TRUE)
}

#' Computes monthly corrections for a given diffs table
#'
#' @param diffs_table A daily diffs table, consisting of a tsibble with a column for each match_id
#' @return A tibble with a column for each match_id, containing the monthly correction
monthly_corrections <- function(diffs_table) {
    diffs_table |>
        index_by(ymt = ~ yearmonth(.)) |>
        summarise(across(everything(), ~ mean(., na.rm = TRUE))) |>
        index_by(mt = ~ month(.)) |>
        summarise(across(everything(), ~ mean(., na.rm = TRUE))) |>
        mutate(t = annual_index(date)) |>
        as_tibble() |>
        select(-ymt, -mt)
}

prepare_for_modeling <- function(monthly_corrections) {
    monthly_corrections |>
        pivot_longer(cols = !c(date, t), names_to = "match_id", values_to = "diffs") |>
        arrange(match_id, t)
}

model_and_predict_corrections <- function(monthly_diffs, t, orig_t) {
    n_data <- length(monthly_diffs |> na.omit())
    if (n_data >= 8) {
        model <- lm(monthly_diffs ~ sin(t / 2) + sin(t) + sin(3 * t / 2))
        predict(model, newdata = orig_t) |> unname()
    } else if (n_data > 2) {
        rep(mean(monthly_diffs, na.rm = TRUE), times = nrow(orig_t))
    } else {
        rep(0, times = nrow(orig_t))
    }
}

coalesce_group <- function(id.x, ids.y, match_ids, data_table, corrections) {
    replacement_values <- select(data_table, all_of(ids.y)) + select(corrections, all_of(match_ids))
    coalesce(pull(data_table, id.x), !!!replacement_values)
}

merge_database_by <- function(match_list, data_table, .test_bounds = NULL, ...) {
    data_table <- fill_gaps(data_table) |>
        as_tibble() |>
        mutate(t = annual_index(date)) |>
        arrange(date)
    #  Computing a correction table for each match
    corrections <- diffs_table(match_list, data_table) |>
        monthly_corrections() |>
        reframe(
            across(!c(date, t), ~ model_and_predict_corrections(., t, data_table)),
            t = data_table$t
        )
    if (!is.null(.test_bounds)) {
        # Asserting that the corrections are reasonable in absolute value
        corrections |>
            assertr::assert(
                assertr::within_bounds(-.test_bounds, .test_bounds),
                -t
            )
    }

    match_list |>
        arrange(station_id.x, desc(f0), abs(monthlydelT), desc(valid_days_inters), ...) |>
        group_by(variable, station_id.x) |>
        reframe(
            value = coalesce_group(paste0(first(variable), "_", first(station_id.x)), paste0(variable, "_", station_id.y), match_id, data_table, corrections),
            date = data_table$date,
            merged = !(is.na(value) | !is.na(pull(data_table, paste0(first(variable), "_", first(station_id.x))))),
        ) |>
        drop_na(value) |>
        rename(station_id = station_id.x)
}

filter_remaining_ <- function(target_list, control_list, match_list) {
    target_list |>
        anti_join(match_list, by = join_by(station_id == station_id.x)) |>
        anti_join(match_list, by = join_by(station_id == station_id.y)) |>
        semi_join(control_list, by = "station_id")
}

#' Combines the data and metadata of merged and not merged series.
#'
#' Replaces the data and metadata of the ancestors of the merged series with the merged data and metadata.
#'
#' @param merged_data A dataframe/Table containing the merged series data.
#' @param whole_database A database containing the data and metadata of the whole database.
#' @param approved_match_list A match table, as returned by \code{\link{match_table}}, complete with two `station_id` and a `variable` columns.
#' @param check_consistency Whether to check the consistency of the resulting database.
#'
#' @return A database containing the data and metadata of the merged dataset.
concat_merged_and_unmerged <- function(merged_data, whole_database, approved_match_list, check_consistency = TRUE) {
    # Only station metadata from the left (.x) database is kept for merged series.
    merged_meta <- whole_database$meta |>
        semi_join(approved_match_list, join_by(station_id == station_id.x))

    # Must pay attention to properly manage the "variable"-"station_id" key.
    nonmerged_data <- whole_database$data |>
        anti_join(approved_match_list, join_by(station_id == station_id.x, variable)) |>
        anti_join(approved_match_list, join_by(station_id == station_id.y, variable))

    concat_data <- concat_tables(merged_data |> compute(), nonmerged_data |> compute(), unify_schemas = FALSE)
    concat_meta <- whole_database$meta |> semi_join(concat_data, by = "station_id")

    if (check_consistency) {
        if (concat_data |> collect() |> duplicates(key = c(station_id, variable), index = date) |> nrow() > 0) {
            stop("Duplicates were found in the concatenated data")
        }
        concat_meta |>
            collect() |>
            assert(is_uniq, station_id)
    }

    as_database.ArrowTabular(concat_meta, concat_data)
}
