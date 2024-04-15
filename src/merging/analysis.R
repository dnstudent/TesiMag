library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)

source("src/merging/pairing.R")
source("src/analysis/data/clim_availability.R")

lag_analysis <- function(x, series_matches, time_offsets) {
    series_matches <- copy_to(x$src$con, series_matches, overwrite = TRUE)

    time_offsets <- tibble(offset_days = as.integer(time_offsets))
    lagged_match_list <- series_matches |>
        select(starts_with("key"), variable) |>
        cross_join(time_offsets, copy = TRUE) |>
        compute()

    best_lagged_matches <- pair_common_series(x, lagged_match_list, copy = FALSE) |>
        group_by(key_x, key_y, variable, offset_days) |>
        summarise(f0 = mean(as.integer(abs(value_y - value_x) < 1e-4), na.rm = TRUE), analyse = n() > 360L, .groups = "drop_last") |>
        mutate(analyse = any(analyse, na.rm = TRUE)) |>
        slice_max(f0, with_ties = TRUE, .preserve = TRUE) |>
        filter(n() == 1L) |>
        ungroup() |>
        mutate(offset_days = if_else(analyse, offset_days, 0L)) |>
        select(-f0, -analyse) |>
        compute()

    nodata_matches <- series_matches |>
        anti_join(best_lagged_matches, by = c("key_x", "key_y", "variable")) |>
        mutate(offset_days = 0L) |>
        select(colnames(best_lagged_matches)) |>
        compute()

    rows_append(best_lagged_matches, nodata_matches) |> collect()
}

climatology_avail_statistics <- function(paired_series, minimum_valid_days = 20L, maximum_consecutive_missing_days = 4L, n_years_threshold = 10L) {
    paired_series |>
        mutate(value = na_if(!is.na(value_x) | !is.na(value_y), FALSE)) |>
        select(key_x, key_y, variable, date, value) |>
        group_by(key_x, key_y, variable, month = as.integer(month(date)), year = as.integer(year(date))) |>
        monthly_availabilities.grouped(minimum_valid_days, maximum_consecutive_missing_days) |>
        group_by(key_x, key_y, variable, month) |>
        clim_availability.grouped(n_years_threshold)
}

climatic_statistics <- function(paired_series) {
    paired_series |>
        group_by(key_x, key_y, variable, month = month(date)) |>
        summarise(value_x = mean(value_x, na.rm = TRUE), value_y = mean(value_y, na.rm = TRUE), .groups = "drop") |>
        mutate(difference = value_y - value_x) |>
        group_by(key_x, key_y, variable) |>
        summarise(
            climaticdelT = mean(difference, na.rm = TRUE),
            climaticmaeT = mean(abs(difference), na.rm = TRUE),
            climaticsdT = sd(difference, na.rm = TRUE),
            .groups = "drop"
        )
}

yearmonthly_statistics <- function(paired_series) {
    paired_series |>
        group_by(key_x, key_y, variable, year = year(date), month = month(date)) |>
        summarise(value_x = mean(value_x, na.rm = TRUE), value_y = mean(value_y, na.rm = TRUE), .groups = "drop") |>
        mutate(difference = value_y - value_x) |>
        group_by(key_x, key_y, variable) |>
        summarise(
            monthlydelT = mean(difference, na.rm = TRUE),
            monthlymaeT = mean(abs(difference), na.rm = TRUE),
            monthlysdT = sd(difference, na.rm = TRUE),
            .groups = "drop"
        )
}

daily_statistics <- function(paired_series, epsilon) {
    paired_series |>
        mutate(
            difference = value_y - value_x,
            valkey_x = !is.na(value_x),
            valkey_y = !is.na(value_y),
            ints = (abs(value_x - trunc(value_x)) < 1e-4) & (abs(value_y - trunc(value_y)) < 1e-4),
            nointsdiff = if_else(ints, NA, difference)
        ) |>
        mutate(difference = if_else(abs(difference) <= epsilon, 0, difference)) |>
        group_by(key_x, key_y, variable) |>
        summarise(
            maeT = mean(abs(difference), na.rm = TRUE),
            delT = mean(difference, na.rm = TRUE),
            sdT = sd(difference, na.rm = TRUE),
            valid_days_x = sum(as.integer(valkey_x), na.rm = TRUE),
            valid_days_y = sum(as.integer(valkey_y), na.rm = TRUE),
            valid_days_inters = sum(as.integer(valkey_x & valkey_y), na.rm = TRUE),
            valid_days_union = sum(as.integer(valkey_x | valkey_y), na.rm = TRUE),
            f0 = mean(as.integer(abs(difference) <= epsilon), na.rm = TRUE),
            balance = mean(na_if(sign(difference), 0), na.rm = TRUE),
            fsameint = mean(as.integer(abs(trunc(value_y) - trunc(value_x)) < 0.5), na.rm = TRUE),
            f0noint = mean(as.integer(abs(nointsdiff) <= epsilon), na.rm = TRUE),
            .groups = "drop"
        ) |>
        mutate(
            overlap_min = valid_days_inters / pmin(valid_days_x, valid_days_y, na.rm = TRUE),
            overlap_max = valid_days_inters / pmax(valid_days_x, valid_days_y, na.rm = TRUE),
            overlap_union = valid_days_inters / valid_days_union,
        )
}

metadata_analysis <- function(series_matches, metadata) {
    # metadata <- metadata |> select(
    #     key, name, elevation # , glo30m_elevation, glo30asec_elevation
    # )
    series_matches |>
        left_join(metadata, join_by(key_x == key)) |>
        left_join(metadata, join_by(key_y == key), suffix = c("_x", "_y")) |>
        mutate(
            delH = elevation_y - elevation_x,
            # delZm = glo30m_elevation_y - glo30m_elevation_x,
            # delZsec = glo30asec_elevation_y - glo30asec_elevation_x,
            norm_name_x = name_x |> lower() |> replace("_", " ") |> nfc_normalize() |> strip_accents() |> trim(),
            norm_name_y = name_y |> lower() |> replace("_", " ") |> nfc_normalize() |> strip_accents() |> trim(),
            strSym = jaro_winkler_similarity(norm_name_y, norm_name_x),
            common_period = pmax(as.integer(pmin(sensor_last_x, sensor_last_y, na.rm = TRUE) - pmax(sensor_first_x, sensor_first_y, na.rm = TRUE)), 0L, na.rm = TRUE),
            common_period_vs_x = common_period / (sensor_last_x - sensor_first_x),
            common_period_vs_y = common_period / (sensor_last_y - sensor_first_y),
        )
}

periodicity_analysis <- function(paired_series) {
    diffs <- paired_series |>
        mutate(delT = value_y - value_x, .keep = "unused") |>
        filter(!is.na(delT) & abs(delT) > 1e-4) |>
        mutate(nextyear = date + years(1))

    diffs |>
        inner_join(diffs, join_by(key_x, key_y, variable, nextyear == date), suffix = c("_0", "_1")) |>
        group_by(key_x, key_y, variable) |>
        summarise(
            selfdiff = mean(abs(delT_0 - delT_1), na.rm = TRUE), .groups = "drop"
        )
}


# FUNZIONE PRINCIPALE
#'
series_matches_analysis <- function(series_matches, data, metadata, matches_offsets = c(-1L, 0L, 1L), epsilon = 1e-4, ...) {
    dbExecute(data$src$con, "DROP TABLE IF EXISTS paired_series")
    matches_offsets <- lag_analysis(data, series_matches, matches_offsets) |>
        left_join(series_matches, by = c("key_x", "key_y", "variable"), relationship = "one-to-one")
    matches_offsets <- copy_to(data$src$con, matches_offsets, overwrite = TRUE)
    # matches_offsets <- duckdb::duckdb_register(data$src$con, "matches_offsets", matches_offsets)
    paired_series <- pair_full_series(data, matches_offsets) |>
        compute(temporary = FALSE, name = "paired_series")

    gc()

    ds <- daily_statistics(paired_series, epsilon)
    ys <- yearmonthly_statistics(paired_series)
    cs <- climatic_statistics(paired_series)
    csa <- climatology_avail_statistics(paired_series, ...)
    md <- metadata_analysis(matches_offsets, metadata)
    # pa <- periodicity_analysis(paired_series)


    analysis <- ds |>
        full_join(ys, by = c("key_x", "key_y", "variable")) |>
        full_join(cs, by = c("key_x", "key_y", "variable")) |>
        full_join(csa, by = c("key_x", "key_y", "variable")) |>
        full_join(md, by = c("key_x", "key_y", "variable")) |>
        # full_join(pa, by = c("key_x", "key_y", "variable")) |>
        collect()

    dbExecute(data$src$con, "DROP TABLE IF EXISTS paired_series")

    analysis
}
