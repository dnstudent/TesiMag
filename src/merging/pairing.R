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

pair_common_series <- function(x, match_list, by = c("key_x", "key_y", "variable"), copy = TRUE) {
    left <- x |>
        rename(key_x = key) |>
        select(any_of(by), date, value) |>
        inner_join(match_list |> select(all_of(by), key_x, variable), by = c("key_x", "variable"), copy = copy)

    right <- x |>
        rename(key_y = key) |>
        select(any_of(by), date, value) |>
        inner_join(match_list |> select(all_of(by), key_y, variable, offset_days), by = c("key_y", "variable"), copy = copy) |>
        mutate(date = date + offset_days)

    inner_join(left, right, by = c(by, "date"), suffix = c("_x", "_y"))
    # if (copy) {
    #     dbExecute(x$src$con, "DROP TABLE lml_tmp_")
    # }
    # r
}
