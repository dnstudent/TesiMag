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


both_are_asym <- function(x, y, value_x, value_y) {
    expr((({{ x }} == {{ value_x }} & {{ y }} == {{ value_y }})))
}

both_are_sym <- function(x, y, value1, value2) {
    expr((!!both_are_asym({{ x }}, {{ y }}, {{ value1 }}, {{ value2 }}) | !!both_are_asym({{ x }}, {{ y }}, {{ value2 }}, {{ value1 }})))
}

tags_are_asym <- function(tag, value_x, value_y, suffix = c("_x", "_y")) {
    tags <- str_c(tag, suffix)
    both_are_asym(!!sym(tags[[1]]), !!sym(tags[[2]]), {{ value_x }}, {{ value_y }})
}

tags_are_sym <- function(tag, value1, value2, suffix = c("_x", "_y")) {
    tags <- str_c(tag, suffix)
    both_are_sym(!!sym(tags[[1]]), !!sym(tags[[2]]), {{ value1 }}, {{ value2 }})
}

in_tags <- function(tag, value, suffix = c("_x", "_y")) {
    tags <- str_c(tag, suffix)
    expr((!!sym(tags[[1]]) == {{ value }} | !!sym(tags[[2]]) == {{ value }}))
}

datasets_are_ <- purrr::partial(tags_are_sym, "dataset")
sensor_keys_are_ <- purrr::partial(tags_are_sym, "sensor_key")
user_codes_are_ <- purrr::partial(tags_are_sym, "user_code")
series_ids_are_ <- purrr::partial(tags_are_sym, "series_id")

datasets_are <- purrr::partial(tags_are_asym, "dataset")
sensor_keys_are <- purrr::partial(tags_are_asym, "sensor_key")
user_codes_are <- purrr::partial(tags_are_asym, "user_code")
series_ids_are <- purrr::partial(tags_are_asym, "series_id")

in_sensor_keys <- purrr::partial(in_tags, "sensor_key")
in_user_codes <- purrr::partial(in_tags, "user_code")
in_series_ids <- purrr::partial(in_tags, "series_id")

one_station_is.sk <- function(dataset, sensor_key) {
    expr(((dataset_x == {{ dataset }} & sensor_key_x == {{ sensor_key }}) | (dataset_y == {{ dataset }} & sensor_key_y == {{ sensor_key }})))
}

one_station_is.uc <- function(dataset, user_code) {
    expr(((dataset_x == {{ dataset }} & user_code_x == {{ user_code }}) | (dataset_y == {{ dataset }} & user_code_y == {{ user_code }})))
}

one_station_is.si <- function(dataset, series_id) {
    expr(((dataset_x == {{ dataset }} & series_id_x == {{ series_id }}) | (dataset_y == {{ dataset }} & series_id_y == {{ series_id }})))
}

one_station_is <- function(dataset, sensor_key = NULL, user_code = NULL, series_id = NULL) {
    if (!is.null(sensor_key)) {
        one_station_is.sk(dataset, sensor_key)
    } else if (!is.null(user_code)) {
        one_station_is.uc(dataset, user_code)
    } else if (!is.null(series_id)) {
        one_station_is.si(dataset, series_id)
    } else {
        stop("You must provide either sensor_key, user_code or series_id")
    }
}
