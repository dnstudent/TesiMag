library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

qc_excursion <- function(data, min_excursion = 0, max_excursion = 50) {
    data |>
        pivot_wider(names_from = variable, values_from = value) |>
        rename(T_MIN = `-1`, T_MAX = `1`) |>
        mutate(excursion = abs(T_MAX - T_MIN)) |>
        mutate(qc_excursion = is.na(excursion) | (0 < excursion & excursion < 50)) |>
        select(-excursion) |>
        pivot_longer(cols = c("T_MIN", "T_MAX"), names_to = "variable", values_to = "value") |>
        mutate(variable = case_match(variable, "T_MIN" ~ -1L, "T_MAX" ~ 1L)) |>
        filter(!is.na(value))
}

qc_gross <- function(data, threshold = 50) {
    data |>
        mutate(qc_gross = is.na(value) | (abs(value) <= threshold))
}

flag_integers <- function(data) {
    data |>
        mutate(is_integer = abs(trunc(value) - value) < 1e-4)
}

assign_repetition_gids <- function(data) {
    data |>
        flag_integers() |>
        group_by(variable, station_id) |>
        window_order(date) |>
        mutate(
            prev_is_different = (abs(value - lag(value)) > 1e-4) |> as.integer() |> coalesce(1L),
            not_consecutive_ints = (!is_integer | !lag(is_integer)) |> as.integer() |> coalesce(1L),
        ) |>
        mutate(
            consecutive_val_gid = cumsum(prev_is_different),
            consecutive_int_gid = cumsum(not_consecutive_ints)
        ) |>
        ungroup() |>
        select(!c(is_integer, prev_is_different, not_consecutive_ints))
}

qc_repetitions <- function(data, threshold = 8L) {
    data |>
        assign_repetition_gids() |>
        group_by(variable, station_id, consecutive_val_gid) |>
        mutate(
            qc_repeated_values = n() < threshold
        ) |>
        ungroup() |>
        group_by(variable, station_id, consecutive_int_gid) |>
        mutate(
            qc_repeated_integers = n() < threshold
        ) |>
        ungroup() |>
        select(!c(consecutive_val_gid, consecutive_int_gid))
}

qc1 <- function(raw_data_tbl) {
    raw_data_tbl |>
        qc_excursion() |>
        qc_gross() |>
        qc_repetitions()
}
