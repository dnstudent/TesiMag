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
source("src/pairing/tools.R")

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

analyze_matches.old <- function(data_table, match_list, metadata_list, same_db = FALSE, climats = FALSE, years_threshold = 10L, dem = stars::read_stars("temp/dem/dem30.tif")) {
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
#' Only common records (i.e. same variable and date) are kept.
match_list_to_semi_records <- function(match_list, database, y_offset) {
    join_data_on_matchlist(
        database$data |> select(-merged),
        match_list,
        database$data |> select(-merged) |> mutate(
            date + as.difftime(y_offset * 86400, units = "secs")
        ),
        left_join # Guarantees the presence of a series for each match, even if one side is empty
    ) |>
        arrange(station_id.x, station_id.y, variable, date) |>
        compute()
}

#' Builds a paired series table from the given match list.
#' Assures that all measures in the database are present.
match_list_to_full_ts <- function(offset_match_list, database, offsets) {
    dx <- database$data |>
        rename(station_id.x = station_id) |>
        inner_join(offset_match_list |> select(station_id.x, variable, station_id.y), by = c("station_id.x", "variable")) |>
        select(-merged)

    dy <- database$data |>
        rename(station_id.y = station_id) |>
        inner_join(offset_match_list |> select(station_id.x, variable, station_id.y, offset_days), by = c("station_id.y", "variable")) |>
        mutate(date = as.Date(date + as.difftime(offset_days * 86400, units = "secs"))) |>
        select(-offset_days, -merged)

    full_join(dx, dy, by = c("station_id.x", "station_id.y", "variable", "date")) |>
        arrange(station_id.x, station_id.y, variable, date) |>
        compute()
}

lagged_mae <- function(match_list, database, offset_days) {
    match_list_to_semi_records(match_list |> select(starts_with("station_id.")), database, offset_days) |>
        group_by(station_id.x, station_id.y, variable) |>
        summarise(
            mae = mean(abs(value.y - value.x), na.rm = TRUE), .groups = "drop"
        ) |>
        mutate(offset_days = offset_days, mae = cast(mae, float64())) |>
        compute()
}

#' Suggests an offset for the given matches based on the mae of the daily differences.
lag_analysis <- function(match_list, database) {
    concat_tables(
        lagged_mae(match_list, database, -1),
        lagged_mae(match_list, database, 0),
        lagged_mae(match_list, database, 1),
        unify_schemas = FALSE
    ) |>
        collect() |>
        group_by(station_id.x, station_id.y, variable) |>
        slice_min(mae) |>
        summarise(
            offset_days = mean(offset_days, na.rm = TRUE), .groups = "drop"
        ) |>
        ungroup() |>
        as_arrow_table()
}

add_best_lag <- function(match_list, database, checks = TRUE) {
    original <- match_list
    match_list <- match_list |>
        lag_analysis(database) |>
        left_join(original, by = c("station_id.x", "station_id.y"), relationship = "many-to-one") |> #  There is the variable column
        compute()

    if (checks && original |>
        anti_join(match_list, by = c("station_id.x", "station_id.y")) |>
        compute() |>
        nrow() > 0) {
        stop("There was a problem: some matches were lost")
    }

    match_list
}

paired_records_analysis <- function(paired_data, make_symmetric) {
    summarised <- paired_data |>
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
        ) |>
        compute()

    if (make_symmetric) {
        inverted <- summarised |>
            mutate(delT = -delT) |>
            swap_cols("valid_days.x", "valid_days.y") |>
            swap_cols("station_id.x", "station_id.y") |>
            select(all_of(colnames(summarised))) |>
            compute()
        summarised <- concat_tables(summarised, inverted, unify_schemas = FALSE)
    }
    summarised
}

monthly_series_analysis <- function(paired_monthly_series, make_symmetric) {
    summarised <- paired_monthly_series |>
        mutate(difference = value.y - value.x) |>
        collect() |>
        group_by(station_id.x, station_id.y, variable) |>
        summarise(
            monthlydelT = mean(difference, na.rm = TRUE),
            monthlymaeT = mean(abs(difference), na.rm = TRUE),
            monthlysdT = sd(difference, na.rm = TRUE),
            .groups = "drop"
        ) |>
        ungroup() |>
        as_arrow_table()

    if (make_symmetric) {
        inverted <- summarised |>
            mutate(monthlydelT = -monthlydelT) |>
            swap_cols("station_id.x", "station_id.y") |>
            select(all_of(colnames(summarised))) |>
            compute()
        summarised <- concat_tables(
            summarised,
            inverted,
            unify_schemas = FALSE
        )
    }
    summarised
}

climat_analysis <- function(paired_data, make_symmetric) {
    summarised <- paired_data |>
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
        ) |>
        as_arrow_table(schema = schema(station_id.x = utf8(), station_id.y = utf8(), variable = utf8(), all_filter = bool(), any_filter = bool())) |>
        compute()

    if (make_symmetric) {
        inverted <- summarised |>
            swap_cols("station_id.x", "station_id.y") |>
            select(all_of(colnames(summarised))) |>
            compute()
        summarised <- concat_tables(summarised, inverted, unify_schemas = FALSE)
    }
    summarised
}

metadata_analysis <- function(match_list, database, make_symmetric, dem) {
    if (make_symmetric) {
        match_list <- concat_tables(
            match_list,
            match_list |> swap_cols("station_id.x", "station_id.y") |> mutate(offset_days = -offset_days) |> compute(),
            unify_schemas = FALSE
        )
    }
    match_list |>
        collect() |>
        left_join(database$meta |> collect() |> add_dem_elevations(dem), join_by(station_id.x == station_id), relationship = "many-to-one") |>
        left_join(database$meta |> collect() |> add_dem_elevations(dem), join_by(station_id.y == station_id), relationship = "many-to-one") |>
        add_distance() |>
        mutate(
            H = elevation.x,
            delH = elevation.y - elevation.x,
            delZ = dem.y - dem.x,
            strSym = stringsim(normalize_name(station_name.y), normalize_name(station_name.x), method = "jw")
        ) |>
        as_arrow_table()
}

analyze_matches <- function(match_list, database, first_date, last_date, symmetric, years_threshold = 10L, dem = stars::read_stars("temp/dem/dem30.tif"), checks = TRUE) {
    original_matchlist <- match_list
    if (symmetric) {
        if (checks && !is_symmetric(match_list, "station_id.x", "station_id.y")) {
            stop("The match list is not symmetric...")
        }
        match_list <- filter(match_list, station_id.x > station_id.y)
    }

    # Here the variables get added
    match_list <- match_list |>
        add_best_lag(database, checks)

    # Filtering out matches that do not sussist because the variable is not present

    if (checks && match_list |>
        group_by(station_id.x, station_id.y, variable) |>
        tally() |>
        filter(n != 1L) |>
        compute() |>
        nrow() > 0) {
        stop("There is a problem in the match list: duplicated values")
    }

    paired_data <- match_list_to_full_ts(match_list, database) |> compute()
    if (checks && paired_data |>
        group_by(station_id.x, station_id.y, variable, date) |>
        tally() |>
        filter(n > 1L) |>
        compute() |>
        nrow() > 0) {
        stop("There are duplicates in the paired table")
    }

    monthly_stats <- paired_data |>
        group_by(station_id.x, station_id.y, variable, month = month(date), year = year(date)) |>
        summarise(value.x = mean(value.x, na.rm = TRUE), value.y = mean(value.y, na.rm = TRUE)) |>
        ungroup() |>
        monthly_series_analysis(make_symmetric = symmetric)

    paired_series_stats <- paired_records_analysis(paired_data, make_symmetric = symmetric)
    climat_stats <- climat_analysis(paired_data, make_symmetric = symmetric)

    # Using full joins to easily detect errors down the line
    Tstats <- full_join(paired_series_stats, monthly_stats, by = c("station_id.x", "station_id.y", "variable"), relationship = "one-to-one") |>
        full_join(climat_stats, by = c("station_id.x", "station_id.y", "variable"), relationship = "one-to-one")

    metadata <- metadata_analysis(match_list, database, make_symmetric = symmetric, dem)

    result <- full_join(Tstats, metadata, by = c("station_id.x", "station_id.y", "variable"), relationship = "one-to-one") |>
        collect() |>
        mutate(match_id = as.character(row_number())) |>
        # Ugly patch to avoid matches with empty (or too short) series
        filter(valid_days.y > 30 & valid_days.x > 30)

    # Checking that the match list still lists the original matches
    if (checks &&
        (
            original_matchlist |>
                anti_join(result, by = c("station_id.x", "station_id.y")) |>
                compute() |>
                nrow() > 0 ||
                result |>
                    group_by(station_id.x, station_id.y, variable) |>
                    tally() |>
                    filter(n != 1L) |>
                    compute() |>
                    nrow() > 0
        )
    ) {
        warn("There was a problem: some matches were lost. This could be do to matches with empty series")
    }
    result
}
