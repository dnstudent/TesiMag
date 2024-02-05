library(dplyr, warn.conflicts = FALSE)

pair_full_series <- function(data, lagged_match_list, by = c("key_x", "key_y", "variable")) {
    left <- data |>
        rename(key_x = key) |>
        select(any_of(by), date, value) |>
        inner_join(lagged_match_list |> select(all_of(by), key_x, variable), by = c("key_x", "variable"), copy = TRUE)

    right <- data |>
        rename(key_y = key) |>
        select(any_of(by), date, value) |>
        inner_join(lagged_match_list |> select(all_of(by), key_y, variable, offset_days), by = c("key_y", "variable"), copy = TRUE) |>
        mutate(date = date + offset_days) |>
        select(-offset_days)

    full_join(left, right, by = c(by, "date"), suffix = c("_x", "_y"))
}

pair_common_series <- function(x, lagged_match_list) {
    left <- x |>
        inner_join(lagged_match_list |> select(starts_with("key"), variable), by = c("key" = "key_x", "variable")) |>
        rename(key_x = key)

    right <- x |>
        inner_join(lagged_match_list |> select(starts_with("key"), variable, offset_days), by = c("key" = "key_y", "variable")) |>
        mutate(date = date + offset_days) |>
        rename(key_y = key)

    inner_join(left, right, by = c("key_x", "key_y", "date", "variable"), suffix = c("_x", "_y"))
}
