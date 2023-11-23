library(dplyr, warn.conflicts = FALSE)

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
        mutate(t = yday(date) / yday(ceiling_date(date, unit = "year") - 1)) |>
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
    model <- lm(diffs ~ sin(pi * t) + sin(2 * pi * t) + sin(3 * pi * t), data = monthly_diffs)
    bind_cols(correction = predict(model, ts), date = ts$date)
}

update_left <- function(match_table, data.x, data.y) {
    data.x <- mutate(data.x, t = yday(data.x$date) / yday(ceiling_date(data.x$date, unit = "year") - 1))
    corrections <- diffs_table(match_table, data.x, data.y) |>
        monthly_corrections() |>
        prepare_for_modeling() |>
        group_by(match_id) |>
        group_modify(~ model_and_predict_corrections(., data.x |> select(t, date)))
    merged_series <- match_table |>
        select(identifier.x, identifier.y, match_id) |>
        rowwise() |>
        reframe(
            value = coalesce(data.x |> pull(identifier.x), data.y |> pull(identifier.y)),
            date = data.x$date,
            match_id = as.character(match_id),
            from.y = is.na(data.x |> pull(identifier.x)) & !is.na(data.y |> pull(identifier.y))
        )
    inner_join(merged_series, corrections, by = c("date", "match_id"), relationship = "one-to-one") |> mutate(value = if_else(from.y, value + correction, value))
}

update_tables <- function(data1, id1, data2, id2) {
    data1 |> update(id1, data2$id2)
}
