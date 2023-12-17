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
minimal_overlap <- function(s1, s2) {
    sum(!(is.na(s1) | is.na(s2))) / min(length(s1 |> na.omit()), length(s2 |> na.omit()))
}

#' The fraction of data that is either missing in both series or present in both series. Series must be of the same length.
#'
#' @param s1 A time series.
#' @param s2 A time series.
overlap <- function(s1, s2) {
    1 - sum(xor(is.na(s1), is.na(s2))) / length(s1)
}

Tinfo.numeric <- function(s1, s2) {
    difference <- s1 - s2
    valid1 <- !is.na(s1)
    valid2 <- !is.na(s2)
    tibble(
        maeT = mean(abs(difference), na.rm = TRUE),
        delT = mean(difference, na.rm = TRUE),
        sdT = sd(difference, na.rm = TRUE),
        corT = cor(s1, s2, use = "na.or.complete"),
        overlap = overlap(s1, s2),
        minilap = minimal_overlap(s1, s2),
        valid_days.x = sum(valid1),
        valid_days.y = sum(valid2),
        valid_days_inters = sum(valid1 & valid2),
        valid_days_union = sum(valid1 | valid2),
        f0 = mean(abs(difference) <= 1e-4, na.rm = TRUE),
        fplus = mean(difference > 1e-4, na.rm = TRUE),
        fsemiside = max(fplus + f0, 1 - (fplus + f0)),
        fsameint = mean(abs(trunc(s1) - trunc(s2)) < 0.5, na.rm = TRUE)
    ) |> select(-fplus)
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


analyze_matches.old <- function(match_list, data_table, metadata_list, climats = FALSE, years_threshold = 10L) {
    ymonthly <- function(table) {
        table |>
            index_by(ymt = ~ yearmonth(.)) |>
            summarise(across(!where(is.Date), ~ mean(., na.rm = TRUE)))
    }

    data_table.ymonthly <- ymonthly(data_table)
    # table.y.ymonthly <- ymonthly(table.y)

    # if (climats) {
    #     climat <- function(table) {
    #         table |>
    #             index_by(month = ~ month(.)) |>
    #             summarise(across(everything(), ~ mean(., na.rm = TRUE)))
    #     }
    #     table.x.climat <- climat(data_table.ymonthly)
    #     table.y.climat <- climat(table.y.ymonthly)
    #     matches <- match_list |>
    #         rowwise() |>
    #         mutate(
    #             climatcorT = cor(pull(table.x.climat, identifier.x), pull(table.y.climat, identifier.y), use = "na.or.complete"),
    #         )
    # }

    ### METADATA
    match_list <- bijoin(match_list, metadata_list) |>
        collect() |>
        add_distance()

    match_list |>
        mutate(
            delH = abs(elevation.x - elevation.y),
            delZ = abs(dem.x - dem.y),
            strSym = stringsim(normalize_name(station_name.x), normalize_name(station_name.y), method = "jw")
        ) |>
        rowwise() |>
        mutate(
            Tinfo = Tinfo.numeric(pull(data_table, series_id.x), pull(data_table, series_id.y)),
            monthlyslopeT = slope_diff(pull(data_table.ymonthly, series_id.x), pull(data_table.ymonthly, series_id.y)),
            monthlydelT = mean(pull(data_table.ymonthly, series_id.x) - pull(data_table.ymonthly, series_id.y), na.rm = TRUE),
            monthlymae = mean(abs(pull(data_table.ymonthly, series_id.x) - pull(data_table.ymonthly, series_id.y)), na.rm = TRUE),
            monthlysdT = sd(pull(data_table.ymonthly, series_id.x) - pull(data_table.ymonthly, series_id.y), na.rm = TRUE),
            climat_availability = is_climatology_computable.series(
                pull(data_table, series_id.x),
                pull(data_table, series_id.y),
                data_table$date,
                n_years_minimum = years_threshold
            ) |> as_tibble() |> summarise(all_filter = all(clim_available), any_filter = any(clim_available))
        ) |>
        unnest(c(Tinfo, climat_availability))
}

analyze_matches <- function(data_table, match_list, metadata_list, climats = FALSE, years_threshold = 10L, dem = stars::read_stars("temp/dem/dem30.tif")) {
    data_table.ymonthly <- data_table |>
        index_by(ymt = ~ yearmonth(.)) |>
        summarise(across(!where(is.Date), ~ mean(., na.rm = TRUE)))

    ### METADATA
    match_list <- bijoin(match_list |> collect(), metadata_list |> collect() |> add_dem_elevations(dem)) |>
        add_distance() |>
        mutate(
            delH = abs(elevation.x - elevation.y),
            delZ = abs(dem.x - dem.y),
            strSym = stringsim(normalize_name(station_name.x), normalize_name(station_name.y), method = "jw")
        )
    match_list <- bind_rows(
        T_MIN = match_list,
        T_MAX = match_list,
        .id = "variable"
    ) |> mutate(match_id = as.character(row_number()))

    match_list |>
        rowwise() |>
        mutate(
            Tinfo = Tinfo.numeric(pull(data_table, paste0(variable, "_", station_id.x)), pull(data_table, paste0(variable, "_", station_id.y))),
            monthlyslopeT = slope_diff(pull(data_table.ymonthly, paste0(variable, "_", station_id.x)), pull(data_table.ymonthly, paste0(variable, "_", station_id.y))),
            monthlydelT = mean(pull(data_table.ymonthly, paste0(variable, "_", station_id.x)) - pull(data_table.ymonthly, paste0(variable, "_", station_id.y)), na.rm = TRUE),
            monthlymae = mean(abs(pull(data_table.ymonthly, paste0(variable, "_", station_id.x)) - pull(data_table.ymonthly, paste0(variable, "_", station_id.y))), na.rm = TRUE),
            monthlysdT = sd(pull(data_table.ymonthly, paste0(variable, "_", station_id.x)) - pull(data_table.ymonthly, paste0(variable, "_", station_id.y)), na.rm = TRUE),
            climat_availability = is_climatology_computable.series(
                pull(data_table, paste0(variable, "_", station_id.x)),
                pull(data_table, paste0(variable, "_", station_id.y)),
                data_table$date,
                n_years_minimum = years_threshold
            ) |> as_tibble() |> summarise(all_filter = all(clim_available), any_filter = any(clim_available))
        ) |>
        unnest(c(Tinfo, climat_availability))
}
