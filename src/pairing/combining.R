library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(stringi, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)

source("src/database/write.R")

annual_index <- function(date) {
    2 * pi * yday(date) / yday(ceiling_date(date, unit = "year") - 1)
}

#' Computes a diffs table for a given match table, loading the data series from the given tables.
#'
#' @param match_list A match table, as returned by \code{\link{match_table}}, complete with two series identifer columns and a match_id column.
#' @param data_table A table containing the data series for the first database, one per column, and a date index.
#' @return A tsibble with a column for each match_id, containing the difference between the two series linked to the id.
diffs_table <- function(match_list, data_table) {
    match_list |>
        select(series_id.x, series_id.y, match_id) |>
        rowwise() |>
        reframe(diffs = data_table |> pull(series_id.x) - data_table |> pull(series_id.y), date = data_table$date, match_id = match_id) |>
        pivot_wider(id_cols = date, values_from = diffs, names_from = match_id) |>
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

#' Pivots the data to long form and filters out columns with all NA values
prepare_for_modeling <- function(monthly_corrections) {
    monthly_corrections |>
        # select(where(~ !all(is.na(.)))) |>
        pivot_longer(cols = !c(date, t), names_to = "match_id", values_to = "diffs") |>
        arrange(match_id, t)
}

model_and_predict_corrections <- function(monthly_diffs, ts) {
    model <- lm(diffs ~ sin(t) + sin(2 * t) + cos(t) + cos(2 * t), data = monthly_diffs)
    bind_cols(correction = predict(model, ts), date = ts$date)
}

coalesce_group <- function(series_id.x, series_ids.y, match_ids, data_table, corrections) {
    replacement_values <- select(data_table, all_of(series_ids.y)) + select(corrections, all_of(match_ids))
    coalesce(pull(data_table, series_id.x), !!!replacement_values)
}

merge_data <- function(match_list, data_table, ...) {
    data_table <- fill_gaps(data_table) |>
        as_tibble() |>
        mutate(t = annual_index(date)) |>
        arrange(date)
    #  Computing a correction table for each match
    corrections <- diffs_table(match_list, data_table) |>
        monthly_corrections() |>
        prepare_for_modeling() |>
        group_by(match_id) |>
        group_modify(~ model_and_predict_corrections(., data_table |> select(t, date))) |>
        pivot_wider(id_cols = date, names_from = match_id, values_from = correction) |>
        arrange(date) |>
        select(-date)
    match_list |>
        arrange(series_id.x, abs(delT), ...) |>
        group_by(series_id.x) |>
        reframe(
            value = coalesce_group(series_id.x |> first(), series_id.y, match_id, data_table, corrections),
            date = data_table$date,
            merged = !(is.na(value) | !is.na(pull(data_table, series_id.x |> first()))),
        ) |>
        drop_na(value)
}

filter_remaining_ <- function(target_list, control_list, match_list) {
    target_list |>
        anti_join(match_list, by = join_by(series_id == series_id.x)) |>
        anti_join(match_list, by = join_by(series_id == series_id.y)) |>
        semi_join(control_list, by = "series_id")
}

merged_tables <- function(merged_data, original_data_list, series_list, all_matches, dataset_id) {
    c(merged_data, merged_series_metadata) %<-% (left_join(merged_data, series_list |> collect(), join_by(series_id.x == series_id)) |>
        mutate(qc_step = 2L) |>
        name_series(dataset_id) |>
        split_data_metadata())

    remaining_data <- filter_remaining_(original_data_list, series_list, all_matches)
    remaining_series_metadata <- filter_remaining_(series_list, remaining_data, all_matches) |>
        mutate(qc_step = 2L)

    c(remaining_data, remaining_series_metadata) %<-% (left_join(remaining_data, remaining_series_metadata, by = "series_id") |>
        collect() |>
        name_series(dataset_id) |>
        split_data_metadata())

    list(
        concat_tables(merged_data, remaining_data, unify_schemas = FALSE),
        concat_tables(merged_series_metadata, remaining_series_metadata, unify_schemas = FALSE)
    )

    # list(merged_data, remaining_data, merged_series_metadata, remaining_series_metadata)
}
