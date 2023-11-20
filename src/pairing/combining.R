library(dplyr, warn.conflicts = FALSE)

merge_ts <- function(diffs_tsibble) {
    monthly_corrections <- diffs_tsibble |>
        index_by(ymth = ~ yearmonth(.)) |>
        summarise(diffs = mean(diffs, na.rm = TRUE)) |>
        index_by(mth = ~ month(.)) |>
        summarise(diffs = mean(diffs, na.rm = TRUE))

    # fit <- lm(diffs ~ , )
}

update <- function(data, id1, series2) {
    data |> mutate("{{id1}}" := coalesce({{ id1 }}, series2))
}

update_tables <- function(data1, id1, data2, id2) {
    data1 |> update(id1, data2$id2)
}
