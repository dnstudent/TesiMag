library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

qc_excursion <- function(data, min_excursion = 0, max_excursion = 50) {
    data |>
        pivot_wider(names_from = variable, values_from = value) |>
        rename(T_MIN = `-1`, T_MAX = `1`) |>
        mutate(excursion = T_MAX - T_MIN) |>
        mutate(qc_excursion = is.na(excursion) | (min_excursion <= excursion & excursion <= max_excursion)) |>
        select(-excursion) |>
        pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
        mutate(variable = case_match(variable, "T_MIN" ~ -1L, "T_MAX" ~ 1L)) |>
        filter(!is.na(value))
}

qc_gross <- function(data, min_T = -30, max_T = 50) {
    data |>
        mutate(qc_gross = is.na(value) | (min_T <= value & value <= max_T))
}

flag_integers <- function(data, int_threshold = 1e-4) {
    data |>
        mutate(is_integer = abs(trunc(value) - value) < int_threshold)
}

assign_repetition_gids <- function(data, int_threshold = 1e-4) {
    data |>
        group_by(variable, sensor_key) |>
        window_order(date) |>
        mutate(
            prev_is_different = (abs(value - lag(value)) > int_threshold) |> as.integer() |> coalesce(1L),
        ) |>
        mutate(
            consecutive_val_gid = cumsum(prev_is_different),
        ) |>
        ungroup() |>
        select(!c(prev_is_different))
}

qc_repetitions <- function(data, consecutive_int_threshold = 8L, consecutive_float_threshold = 4L, is_int_threshold = 1e-4) {
    data |>
        assign_repetition_gids(is_int_threshold) |>
        flag_integers() |>
        group_by(variable, sensor_key, consecutive_val_gid) |>
        mutate(
            # qc_repeated_values = n() < threshold
            qc_repeated_values = if_else(all(is_integer, na.rm = TRUE), n() < consecutive_int_threshold, n() < consecutive_float_threshold)
        ) |>
        ungroup() |>
        select(!c(consecutive_val_gid, is_integer))
}

qc1 <- function(raw_data_tbl, min_excursion = 0, max_excursion = 50, min_T = -30, max_T = 50, consecutive_int_threshold = 8L, consecutive_float_threshold = 4L, is_int_threshold = 1e-4) {
    raw_data_tbl |>
        qc_excursion(min_excursion, max_excursion) |>
        qc_gross(min_T, max_T) |>
        qc_repetitions(consecutive_int_threshold, consecutive_float_threshold, is_int_threshold) |>
        mutate(valid = qc_gross & qc_excursion & qc_repeated_values)
}
