library(tidyr, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/analysis/data/clim_availability.R")
source("src/database/query/pairing.R")

# `paired_series` points to a dbplyr query to tesidb with the following columns:
# - id_x: the id of the first station
# - id_y: the id of the second station
# - variable: the variable to compare
# - date: the date of the measurement
# - value_x: the value of the first station
# - value_y: the value of the second station

# `series_matches` points to a dbplyr query to tesidb with the following columns:
# - id_x: the id of the first station
# - id_y: the id of the second station
# - variable: the variable to compare
# - distance: the distance between the two stations

# `metadata_table` points to a dbplyr query to tesidb with the station metadata columns

# `x` points to a dbplyr query to tesidb representing temperature measures with the following columns:
# - station_id: the id of the measuring station
# - variable: the variable measured
# - date: the date of the measurement
# - value: the value of the measurement

climatology_statistics <- function(paired_series, minimum_valid_days = 20L, maximum_consecutive_missing_days = 4L, n_years_threshold = 10L) {
    paired_series |>
        mutate(value = na_if(!is.na(value_x) | !is.na(value_y), FALSE)) |>
        select(id_x, id_y, variable, date, value) |>
        group_by(id_x, id_y, variable, month = as.integer(month(date)), year = as.integer(year(date))) |>
        monthly_availabilities.grouped(minimum_valid_days, maximum_consecutive_missing_days) |>
        group_by(id_x, id_y, variable, month) |>
        clim_availability.grouped(n_years_threshold)
}

yearmonthly_statistics <- function(paired_series) {
    paired_series |>
        group_by(id_x, id_y, variable, year = year(date), month = month(date)) |>
        summarise(value_x = mean(value_x, na.rm = TRUE), value_y = mean(value_y, na.rm = TRUE), .groups = "drop") |>
        mutate(difference = value_y - value_x) |>
        group_by(id_x, id_y, variable) |>
        summarise(
            monthlydelT = mean(difference, na.rm = TRUE),
            monthlymaeT = mean(abs(difference), na.rm = TRUE),
            monthlysdT = sd(difference, na.rm = TRUE),
            .groups = "drop"
        )
}

daily_statistics <- function(paired_series) {
    paired_series |>
        mutate(difference = value_y - value_x, valid_x = !is.na(value_x), valid_y = !is.na(value_y)) |>
        group_by(id_x, id_y, variable) |>
        summarise(
            maeT = mean(abs(difference), na.rm = TRUE),
            delT = mean(difference, na.rm = TRUE),
            sdT = sd(difference, na.rm = TRUE),
            valid_days_x = sum(as.integer(valid_x), na.rm = TRUE),
            valid_days_y = sum(as.integer(valid_y), na.rm = TRUE),
            valid_days_inters = sum(as.integer(valid_x & valid_y), na.rm = TRUE),
            valid_days_union = sum(as.integer(valid_x | valid_y), na.rm = TRUE),
            f0 = mean(as.integer(abs(difference) <= 1e-4), na.rm = TRUE),
            fsameint = mean(as.integer(abs(trunc(value_y) - trunc(value_x)) < 0.5), na.rm = TRUE),
            .groups = "drop"
        )
}

metadata_analysis <- function(series_matches, metadata) {
    metadata <- metadata |> select(
        dataset, id, name, network, state, first_registration, last_registration, elevation, glo30m_elevation, glo30asec_elevation
    )
    series_matches |>
        left_join(metadata, join_by(id_x == id)) |>
        left_join(metadata, join_by(id_y == id), suffix = c("_x", "_y")) |>
        mutate(
            delH = elevation_y - elevation_x,
            delZm = glo30m_elevation_y - glo30m_elevation_x,
            delZsec = glo30asec_elevation_y - glo30asec_elevation_x,
            norm_name_x = name_x |> lower() |> replace("_", " ") |> nfc_normalize() |> strip_accents() |> trim(),
            norm_name_y = name_y |> lower() |> replace("_", " ") |> nfc_normalize() |> strip_accents() |> trim(),
            strSym = jaro_winkler_similarity(norm_name_y, norm_name_x),
        )
}

series_matches_analysis <- function(series_matches, data, metadata, ...) {
    dbExecute(data$src$con, "DROP TABLE IF EXISTS paired_series")
    matches_offsets <- lag_analysis(data, series_matches, c(-1L, 0L, 1L)) |> left_join(series_matches, by = c("id_x", "id_y", "variable"))
    matches_offsets <- copy_to(data$src$con, matches_offsets, overwrite = TRUE)
    # matches_offsets <- duckdb::duckdb_register(data$src$con, "matches_offsets", matches_offsets)
    paired_series <- pair_full_series(data, matches_offsets) |>
        compute(temporary = FALSE, name = "paired_series")

    gc()

    ds <- daily_statistics(paired_series)
    ys <- yearmonthly_statistics(paired_series)
    cs <- climatology_statistics(paired_series, ...)
    md <- metadata_analysis(matches_offsets, metadata)

    analysis <- ds |>
        full_join(ys, by = c("id_x", "id_y", "variable")) |>
        full_join(cs, by = c("id_x", "id_y", "variable")) |>
        full_join(md, by = c("id_x", "id_y", "variable")) |>
        collect()

    dbExecute(data$src$con, "DROP TABLE IF EXISTS paired_series")

    analysis
}
