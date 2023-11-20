library(dplyr, warn.conflicts = FALSE)

diffs_table.multiple <- function(match_table, data.x, data.y) {
    match_table |>
        select(identifier.x, identifier.y, match_id) |>
        rowwise() |>
        reframe(diffs = data.x |> pull(as.character(identifier.x)) - data.y |> pull(as.character(identifier.y)), date = data.x$date, match_id = match_id) |>
        pivot_wider(id_cols = date, values_from = diffs, names_from = match_id) |>
        as_tsibble(index = date) |>
        fill_gaps()
}

monthly_corrections <- function(diffs_table) {
    diffs_table |>
        index_by(ymt = ~ yearmonth(.)) |>
        summarise(across(everything(), ~ mean(., na.rm = TRUE))) |>
        ungroup() |>
        index_by(mt = ~ month(.)) |>
        summarise(across(everything(), ~ mean(., na.rm = TRUE))) |>
        mutate(t = yday(date) / yday(ceiling_date(date, unit = "year") - 1)) |>
        fill_gaps() |>
        as_tibble() |>
        select(-ymt, -mt)
}

model_and_predict_corrections <- function(monthly_diffs, ts) {
    model <- lm(diffs ~ sin(pi * t) + sin(2 * pi * t) + sin(3 * pi * t), data = monthly_diffs)
    bind_cols(correction = predict(model, ts), date = ts$date)
}

merge_scia_dpc <- function(match_table, data.x, data.y) {
    data.x <- mutate(data.x, t = yday(data.x$date) / yday(ceiling_date(data.x$date, unit = "year") - 1))
    corrections <- diffs_table.multiple(match_table, data.x, data.y) |>
        monthly_corrections() |>
        select(where(~ !all(is.na(.)))) |>
        pivot_longer(cols = !c(date, t), names_to = "match_id", values_to = "diffs") |>
        arrange(match_id, t) |>
        group_by(match_id) |>
        group_modify(~ model_and_predict_corrections(., data.x |> select(t, date)))
    merged_series <- match_table |>
        select(identifier.x, identifier.y, match_id) |>
        rowwise() |>
        reframe(value = coalesce(data.x |> pull(as.character(identifier.x)), data.y |> pull(as.character(identifier.y))), date = data.x$date, match_id = as.character(match_id), from.dpc = is.na(data.x |> pull(as.character(identifier.x))))
    inner_join(merged_series, corrections, by = c("date", "match_id"), relationship = "one-to-one") |> mutate(value = if_else(from.dpc, value + correction, value))
}

update_tables <- function(data1, id1, data2, id2) {
    data1 |> update(id1, data2$id2)
}
