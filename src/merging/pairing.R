library(dplyr, warn.conflicts = FALSE)

pair_full_series <- function(data, lagged_match_list, by = c("id_x", "id_y", "variable")) {
    left <- data |>
        rename(id_x = station_id) |>
        # inner_join(lagged_match_list |> select(starts_with("id"), variable), by = c("id_x", "variable"))
        inner_join(lagged_match_list |> select(all_of(by), id_x, variable), by = c("id_x", "variable"), copy = TRUE)

    right <- data |>
        rename(id_y = station_id) |>
        inner_join(lagged_match_list |> select(all_of(by), id_y, variable, offset_days), by = c("id_y", "variable"), copy = TRUE) |>
        mutate(date = date + offset_days) |>
        select(-offset_days)

    full_join(left, right, by = c(by, "date"), suffix = c("_x", "_y"))
}

pair_common_series <- function(x, lagged_match_list) {
    left <- x |>
        inner_join(lagged_match_list |> select(starts_with("id"), variable), by = c("station_id" = "id_x", "variable")) |>
        rename(id_x = station_id)

    right <- x |>
        inner_join(lagged_match_list |> select(starts_with("id"), variable, offset_days), by = c("station_id" = "id_y", "variable")) |>
        mutate(date = date + offset_days) |>
        rename(id_y = station_id)

    inner_join(left, right, by = c("id_x", "id_y", "date", "variable"), suffix = c("_x", "_y"))
}
