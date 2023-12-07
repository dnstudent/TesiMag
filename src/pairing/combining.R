library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(stringi, warn.conflicts = FALSE)

source("src/database/write.R")

annual_index <- function(date) {
    2 * pi * yday(date) / yday(ceiling_date(date, unit = "year") - 1)
}

#' Computes a diffs table for a given match table, loading the data series from the given tables.
#'
#' @param match_table A match table, as returned by \code{\link{match_table}}, complete with two series identifer columns and a match_id column.
#' @param data.x A table containing the data series for the first database, one per column, and a date index.
#' @param data.y A table containing the data series for the second database, one per column. The index must be the same as data.x.
#' @return A tsibble with a column for each match_id, containing the difference between the two series linked to the id.
diffs_table <- function(match_table, data.x, data.y) {
    match_table |>
        select(series_id.x, series_id.y, match_id) |>
        rowwise() |>
        reframe(diffs = data.x |> pull(series_id.x) - data.y |> pull(series_id.y), date = data.x$date, match_id = match_id) |>
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

coalesce_group <- function(series_id.x, series_ids.y, match_ids, data.x, data.y, corrections) {
    replacement_values <- select(data.y, all_of(series_ids.y)) + select(corrections, all_of(match_ids))
    coalesce(pull(data.x, series_id.x), !!!replacement_values)
}

update_group <- function(match_table, data.x, data.y) {

}

# value_from <- function(identifier.x, identifiers.y, data.x, data.y) {
#     integ_data <- select(data.y, identifiers.y)
# }

update_left <- function(match_table, data.x, data.y, ...) {
    data.x <- fill_gaps(data.x) |>
        as_tibble() |>
        mutate(data.x, t = annual_index(date)) |>
        arrange(date)
    data.y <- fill_gaps(data.y) |>
        as_tibble() |>
        arrange(date)
    #  Computing a correction table for each match
    corrections <- diffs_table(match_table, data.x, data.y) |>
        monthly_corrections() |>
        prepare_for_modeling() |>
        group_by(match_id) |>
        group_modify(~ model_and_predict_corrections(., data.x |> select(t, date))) |>
        pivot_wider(id_cols = date, names_from = match_id, values_from = correction) |>
        arrange(date) |>
        select(-date)
    match_table |>
        arrange(series_id.x, abs(delT), ...) |>
        group_by(series_id.x) |>
        reframe(
            value = coalesce_group(series_id.x |> first(), series_id.y, match_id, data.x, data.y, corrections),
            date = data.x$date,
            merged = !(is.na(value) | !is.na(pull(data.x, series_id.x |> first()))),
            # merged_from = list(c(series_id.x, series_id.y) |> as.list())
        ) |>
        drop_na(value)
}

merged_tables <- function(updated_data, data.x, data.y, series.x, series.y, detected_matches) {
    merged_metadata <- updated_data |>
        distinct(series_id.x, merged_from) |>
        mutate(series_id = merged_from |> sapply(stri_flatten) |> sapply(hash) |> unname()) |>
        left_join(series.x |> select(series_id, station_id, variable) |> collect(), join_by(series_id.x == series_id)) |>
        mutate(qc_step = 2L)

    merged_data <- updated_data |>
        left_join(merged_metadata |> select(series_id.x, series_id), by = "series_id.x") |>
        select(-series_id.x) |>
        as_arrow_table2(schema = data_schema)

    remaining.x <- anti_join(data.x, detected_matches, by = join_by(series_id == series_id.x)) |> semi_join(series.x, by = "series_id")
    metadata.x <- semi_join(series.x, remaining.x, by = "series_id") |> mutate(qc_step = 2L)
    remaining.y <- anti_join(data.y, detected_matches, by = join_by(series_id == series_id.y)) |> semi_join(series.y, by = "series_id")
    metadata.y <- semi_join(series.y, remaining.y, by = "series_id") |> mutate(qc_step = 2L)
    merged_metadata <- merged_metadata |>
        select(-series_id.x)
    # as_arrow_table2(series_schema)

    list(merged_data, remaining.x, remaining.y, merged_metadata, metadata.x, metadata.y)

    # list(
    #     concat_tables(merged_data, remaining.x, remaining.y, unify_schemas = FALSE),
    #     concat_tables(merged_metadata, metadata.x, metadata.y, unify_schemas = FALSE)
    # )
}
