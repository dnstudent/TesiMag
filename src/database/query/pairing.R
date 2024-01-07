library(dplyr, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

pair_full_series <- function(x, lagged_match_list, join) {
    left <- x |>
        inner_join(lagged_match_list |> select(starts_with("id"), variable), by = c("station_id" = "id_x", "variable")) |>
        rename(id_x = station_id)

    right <- x |>
        inner_join(lagged_match_list |> select(starts_with("id"), variable, offset_days), by = c("station_id" = "id_y", "variable")) |>
        mutate(date = date + offset_days) |>
        select(-offset_days) |>
        rename(id_y = station_id)

    full_join(left, right, by = c("id_x", "id_y", "date", "variable"), suffix = c("_x", "_y"))
}

pair_common_series <- function(x, lagged_match_list, join) {
    left <- x |>
        inner_join(lagged_match_list |> select(starts_with("id"), variable), by = c("station_id" = "id_x", "variable")) |>
        rename(id_x = station_id)

    right <- x |>
        inner_join(lagged_match_list |> select(starts_with("id"), variable, offset_days), by = c("station_id" = "id_y", "variable")) |>
        mutate(date = date + offset_days) |>
        rename(id_y = station_id)

    inner_join(left, right, by = c("id_x", "id_y", "date", "variable"), suffix = c("_x", "_y"))
}

lag_analysis <- function(x, series_matches, time_offsets) {
    series_matches <- copy_to(x$src$con, series_matches, overwrite = TRUE)

    time_offsets <- tibble(offset_days = as.integer(time_offsets))
    lagged_match_list <- series_matches |>
        select(starts_with("id"), variable) |>
        cross_join(time_offsets, copy = TRUE) |>
        compute()

    best_lagged_matches <- pair_common_series(x, lagged_match_list) |>
        group_by(id_x, id_y, variable, offset_days) |>
        summarise(maeT = mean(abs(value_y - value_x), na.rm = TRUE), .groups = "drop_last") |>
        slice_min(maeT, with_ties = TRUE, .preserve = TRUE) |>
        filter(n() == 1L) |>
        ungroup() |>
        select(-maeT) |>
        compute()

    nodata_matches <- series_matches |>
        anti_join(best_lagged_matches, by = c("id_x", "id_y", "variable")) |>
        mutate(offset_days = 0L) |>
        select(colnames(best_lagged_matches)) |>
        compute()

    rows_append(best_lagged_matches, nodata_matches) |> collect()
}
