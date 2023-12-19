library(stringr, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(tsibble, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)
library(digest, warn.conflicts = FALSE)
library(stringdist, warn.conflicts = FALSE)

source("src/load/load.R")
source("src/pairing/matching.R")

normalize_name <- function(string) {
    string |>
        str_to_lower() |>
        str_remove_all(regex("[^[:lower:]]")) |>
        str_squish()
}

#' The fraction of data, with respects to the shortest series (having excluded NAs), that is not NA. Series must be of the same length.
#'
#' @param s1 A time series.
#' @param s2 A time series.
minimal_overlap <- function(s.x, s.y) {
    sum(!(is.na(s.x) | is.na(s.y))) / min(length(s.x |> na.omit()), length(s.y |> na.omit()))
}

#' The fraction of data that is either missing in both series or present in both series. Series must be of the same length.
#'
#' @param s1 A time series.
#' @param s2 A time series.
overlap <- function(s.x, s.y) {
    1 - sum(xor(is.na(s.x), is.na(s.y))) / length(s.x)
}

mae <- function(s.x, s.y) {
    mean(abs(s.y - s.x), na.rm = TRUE)
}

Tinfo.numeric <- function(s.x, s.y) {
    difference <- s.y - s.x
    valid1 <- !is.na(s.x)
    valid2 <- !is.na(s.y)
    tibble(
        maeT = mean(abs(difference), na.rm = TRUE),
        delT = mean(difference, na.rm = TRUE),
        sdT = sd(difference, na.rm = TRUE),
        corT = cor(s.x, s.y, use = "na.or.complete"),
        overlap = overlap(s.x, s.y),
        minilap = minimal_overlap(s.x, s.y),
        valid_days.x = sum(valid1),
        valid_days.y = sum(valid2),
        valid_days_inters = sum(valid1 & valid2),
        valid_days_union = sum(valid1 | valid2),
        f0 = mean(abs(difference) <= 1e-4, na.rm = TRUE),
        fplus = mean(difference > 1e-4, na.rm = TRUE),
        fsemiside = max(fplus + f0, 1 - (fplus + f0)),
        fsameint = mean(abs(trunc(s.y) - trunc(s.x)) < 0.5, na.rm = TRUE),
    ) |> select(-fplus)
}

Tinfo.incomplete <- function(s.x, s.y, max_timespan) {
    difference <- s.y - s.x
    tibble(
        maeT = mean(abs(difference)),
        delT = mean(difference),
        sdT = sd(difference),
        corT = cor(s.x, s.y),
        overlap = length(s.x) / max_timespan,
        minilap = length(s.x) / max_timespan,
        valid_days_inters = length(s.x),
        f0 = mean(abs(difference) <= 1e-4),
        fplus = mean(difference > 1e-4),
        fsemiside = max(fplus + f0, 1 - (fplus + f0)),
        fsameint = mean(abs(trunc(s.y) - trunc(s.x)) < 0.5),
    ) |> select(-fplus)
}

Toffset.numeric <- function(s.x, s.y) {
    tibble(
        maep1 = mae(head(s.x, -1), tail(s.y, -1)),
        maem1 = mae(tail(s.x, -1), head(s.y, -1)),
    )
}

add_distance <- function(match_list) {
    match_list |> mutate(
        distance = st_distance(
            tibble(lon = lon.x, lat = lat.x) |> st_md_to_sf() |> st_geometry(),
            tibble(lon = lon.y, lat = lat.y) |> st_md_to_sf() |> st_geometry(),
            by_element = TRUE
        ) |> units::drop_units()
    )
}

slope_diff <- function(s1, s2) {
    diffs <- list(x = seq(1, length(s1)), y = s1 - s2)
    if (all(is.na(diffs$y))) {
        NA
    } else {
        lm(y ~ x, diffs, na.action = "na.omit")$coefficients[[2]]
    }
}

analyze_matches <- function(data_table, match_list, metadata_list, same_db = FALSE, climats = FALSE, years_threshold = 10L, dem = stars::read_stars("temp/dem/dem30.tif")) {
    data_table.ymonthly <- data_table |>
        index_by(ymt = ~ yearmonth(.)) |>
        summarise(across(!where(is.Date), ~ mean(., na.rm = TRUE)))

    ### METADATA
    match_list <- bijoin_metadata_on_matchlist(match_list |> collect(), metadata_list |> collect() |> add_dem_elevations(dem)) |>
        add_distance() |>
        mutate(
            H = elevation.x,
            delH = abs(elevation.y - elevation.x),
            delZ = abs(dem.y - dem.x),
            strSym = stringsim(normalize_name(station_name.y), normalize_name(station_name.x), method = "jw")
        )
    match_list <- bind_rows(
        T_MIN = match_list,
        T_MAX = match_list,
        .id = "variable"
    ) |> mutate(match_id = as.character(row_number()))

    analysis <- match_list |>
        rowwise() |>
        mutate(
            Tinfo = Tinfo.numeric(pull(data_table, paste0(variable, "_", station_id.x)), pull(data_table, paste0(variable, "_", station_id.y))),
            Toffset = Toffset.numeric(pull(data_table, paste0(variable, "_", station_id.x)), pull(data_table, paste0(variable, "_", station_id.y))),
            monthlydelT = mean(pull(data_table.ymonthly, paste0(variable, "_", station_id.y)) - pull(data_table.ymonthly, paste0(variable, "_", station_id.x)), na.rm = TRUE),
            monthlymaeT = mean(abs(pull(data_table.ymonthly, paste0(variable, "_", station_id.y)) - pull(data_table.ymonthly, paste0(variable, "_", station_id.x))), na.rm = TRUE),
            monthlysdT = sd(pull(data_table.ymonthly, paste0(variable, "_", station_id.y)) - pull(data_table.ymonthly, paste0(variable, "_", station_id.x)), na.rm = TRUE),
            climat_availability = is_climatology_computable.series(
                pull(data_table, paste0(variable, "_", station_id.x)),
                pull(data_table, paste0(variable, "_", station_id.y)),
                data_table$date,
                n_years_minimum = years_threshold
            ) |> as_tibble() |> summarise(all_filter = all(clim_available), any_filter = any(clim_available))
        ) |>
        unnest(c(Tinfo, Toffset, climat_availability))

    if (same_db) {
        analysis <- analysis |>
            filter(
                (dataset_id.x != dataset_id.y) |
                    ((valid_days.x >= valid_days.y)) # Filtering out (almost all) duplicates
            )
    }
    analysis
}

#' Builds a paired series table from the given match list.
#' Only common records (in variable and date) are kept.
match_list_to_overlapping_records <- function(match_list, database, y_offset) {
    dx <- database$data |>
        select(-merged) |>
        inner_join(match_list, join_by(station_id == station_id.x)) |>
        rename(station_id.x = station_id)

    dy <- database$data |>
        select(-merged) |>
        inner_join(match_list, join_by(station_id == station_id.y)) |>
        rename(station_id.y = station_id) |>
        mutate(date = as.Date(date + as.difftime(y_offset * 86400, units = "secs")))

    inner_join(dx, dy, by = c("station_id.x", "station_id.y", "variable", "date")) |>
        arrange(station_id.x, station_id.y, variable, date) |>
        compute()
}

#' Builds a paired series table from the given match list.
#' Assures that all measures in the database are present.
match_list_to_full_join <- function(match_list, database) {
    dx <- database$data |>
        inner_join(match_list |> select(station_id.x, station_id.y, variable), join_by(station_id == station_id.x, variable)) |>
        rename(station_id.x = station_id) |>
        select(-merged)

    dy <- database$data |>
        select(-merged) |>
        inner_join(match_list |> select(station_id.x, station_id.y, variable, offset), join_by(station_id == station_id.y, variable)) |>
        rename(station_id.y = station_id) |>
        mutate(date = as.Date(date + as.difftime(offset * 86400, units = "secs"))) |>
        select(-offset)

    full_join(dx, dy, c("station_id.x", "station_id.y", "variable", "date")) |>
        arrange(station_id.x, station_id.y, variable, date) |>
        compute()
}

paired_records_analysis <- function(paired_data) {
    paired_data |>
        mutate(difference = value.y - value.x, valid.x = !is.na(value.x), valid.y = !is.na(value.y)) |>
        group_by(station_id.x, station_id.y, variable) |>
        summarise(
            maeT = mean(abs(difference), na.rm = TRUE),
            delT = mean(difference, na.rm = TRUE),
            sdT = sd(difference, na.rm = TRUE),
            valid_days.x = sum(valid.x),
            valid_days.y = sum(valid.y),
            valid_days_inters = sum(valid.x & valid.y),
            valid_days_union = sum(valid.x | valid.y),
            f0 = mean(abs(difference) <= 1e-4, na.rm = TRUE),
            fsameint = mean(abs(trunc(value.y) - trunc(value.x)) < 0.5, na.rm = TRUE),
            .groups = "drop"
        )
}

monthly_series_analysis <- function(paired_monthly_series) {
    paired_monthly_series |>
        group_by(station_id.x, station_id.y, variable) |>
        summarise(
            monthlydelT = mean(value.y - value.x, na.rm = TRUE),
            monthlymaeT = mean(abs(value.y - value.x), na.rm = TRUE),
            monthlysdT = sd(value.y - value.x, na.rm = TRUE),
            .groups = "drop"
        )
}

lag_mae <- function(match_list, database, offset) {
    r <- match_list_to_overlapping_records(match_list, database, offset) |>
        group_by(station_id.x, station_id.y, variable) |>
        summarise(
            mae = mean(abs(value.y - value.x), na.rm = TRUE), .groups = "drop"
        ) |>
        mutate(offset = offset, mae = cast(mae, float64())) |>
        compute()
}

#' Suggests an offset for the given matches based on the mae of the daily differences.
lag_analysis <- function(match_list, database) {
    concat_tables(
        lag_mae(match_list, database, -1),
        lag_mae(match_list, database, 0),
        lag_mae(match_list, database, 1),
        unify_schemas = FALSE
    ) |>
        collect() |>
        group_by(station_id.x, station_id.y, variable) |>
        slice_min(mae) |>
        group_by(station_id.x, station_id.y, variable) |>
        arrange(abs(offset)) |>
        slice_head() |>
        ungroup()
}

prepare_match_list <- function(match_list, database) {
    lag_data <- match_list |>
        as_arrow_table() |>
        lag_analysis(database) |>
        collect()
    elided <- anti_join(match_list, lag_data, by = c("station_id.x", "station_id.y")) |> collect()
    elided <- bind_rows(
        T_MIN = elided,
        T_MAX = elided,
        .id = "variable"
    ) |> mutate(offset = 0)
    bind_rows(lag_data, elided |> anti_join(lag_data, by = c("station_id.x", "station_id.y", "variable"))) |> as_arrow_table()
}

analyze_matches.hmm <- function(match_list, database, first_date, last_date, years_threshold = 10L, dem = stars::read_stars("temp/dem/dem30.tif")) {
    match_list <- prepare_match_list(match_list, database)
    if (match_list |> group_by(station_id.x, station_id.y, variable) |> tally() |> filter(n > 1) |> compute() |> nrow() > 1) {
        stop("There is a problem in the match list: duplicated values")
    }

    paired_data <- match_list_to_full_join(match_list, database) |> compute()
    if (paired_data |> collect() |> duplicates(key = c(station_id.x, station_id.y, variable), index = date) |> nrow() > 1) {
        stop("There are duplicates in the paired table")
    }

    monthly_stats <- paired_data |>
        group_by(station_id.x, station_id.y, variable, year = year(date), month = month(date)) |>
        summarise(value.x = mean(value.x, na.rm = TRUE), value.y = mean(value.y, na.rm = TRUE), .groups = "drop") |>
        mutate(difference = value.y - value.x) |>
        collect() |> # There are imperscrutable reasons for which this is necessary
        group_by(station_id.x, station_id.y, variable) |>
        summarise(
            monthlydelT = mean(difference, na.rm = TRUE),
            monthlymaeT = mean(abs(difference), na.rm = TRUE),
            monthlysdT = sd(difference, na.rm = TRUE),
            .groups = "drop"
        ) |>
        as_arrow_table()

    paired_series_stats <- paired_records_analysis(paired_data)
    Tstats <- left_join(paired_series_stats, monthly_stats, by = c("station_id.x", "station_id.y", "variable"))
    climat_infos <- paired_data |>
        collect() |>
        mutate(mixed_var = na_if(!is.na(value.x) | !is.na(value.y), FALSE)) |>
        select(station_id.x, station_id.y, variable, date, mixed_var) |>
        as_tsibble(key = c(station_id.x, station_id.y, variable), index = date) |>
        is_climatology_computable.tbl_ts(
            mixed_var,
            .start = first_date, .end = last_date
        ) |>
        as_tibble() |>
        group_by(station_id.x, station_id.y, variable) |>
        summarise(
            all_filter = all(clim_available),
            any_filter = any(clim_available),
            .groups = "drop"
        )


    metadata <- match_list |>
        collect() |>
        left_join(database$meta |> collect() |> add_dem_elevations(dem), join_by(station_id.x == station_id)) |>
        left_join(database$meta |> collect() |> add_dem_elevations(dem), join_by(station_id.y == station_id)) |>
        add_distance() |>
        mutate(
            H = elevation.x,
            delH = abs(elevation.y - elevation.x),
            delZ = abs(dem.y - dem.x),
            strSym = stringsim(normalize_name(station_name.y), normalize_name(station_name.x), method = "jw")
        )

    full_join(Tstats, climat_infos, by = c("station_id.x", "station_id.y", "variable")) |>
        full_join(metadata, by = c("station_id.x", "station_id.y", "variable")) |>
        collect() |>
        mutate(match_id = as.character(row_number()))
}
