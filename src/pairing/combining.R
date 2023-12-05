library(dplyr, warn.conflicts = FALSE)

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
        select(identifier.x, identifier.y, match_id) |>
        rowwise() |>
        reframe(diffs = data.x |> pull(identifier.x) - data.y |> pull(identifier.y), date = data.x$date, match_id = match_id) |>
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

coalesce_group <- function(identifier.x, identifiers.y, match_ids, data.x, data.y, corrections) {
    replacement_values <- select(data.y, all_of(identifiers.y)) + select(corrections, all_of(match_ids))
    coalesce(pull(data.x, identifier.x), !!!replacement_values)
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
        arrange(identifier.x, abs(delT), ...) |>
        # rowwise() |>
        # reframe(
        #     value = coalesce(data.x |> pull(identifier.x), data.y |> pull(identifier.y), .size = nrow(data.x)),
        #     date = data.x$date,
        #     match_id = as.character(match_id),
        #     from.y = is.na(data.x |> pull(identifier.x)) & !is.na(data.y |> pull(identifier.y))
        # )
        group_by(identifier.x) |>
        reframe(
            value = coalesce_group(identifier.x |> first(), identifier.y, match_id, data.x, data.y, corrections),
            date = data.x$date,
            identifier = identifier.x |> first(),
            original_value = is.na(value) | !is.na(pull(data.x, identifier.x |> first())),
            merged_from = list(identifier.y)
        )
    # inner_join(merged_series, corrections, by = c("date", "match_id"), relationship = "one-to-one") |>
    #     mutate(value = if_else(from.y, value + correction, value))
}

update_tables <- function(data1, id1, data2, id2) {
    data1 |> update(id1, data2$id2)
}
