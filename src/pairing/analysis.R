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
        fplus = mean(abs(difference) > 1e-4, na.rm = TRUE),
        fsemiside = max(fplus + f0, 1 - (fplus + f0))
    ) |> select(-fplus)
}



add_distance <- function(match_table, meta.x, meta.y) {
    # p1 <- match_table |>
    #     select(lon = lon.x, lat = lat.x) |>
    #     st_md_to_sf() |>
    #     st_geometry()
    # p2 <- match_table |>
    #     select(lon = lon.y, lat = lat.y) |>
    #     st_md_to_sf() |>
    #     st_geometry()
    # distances <- st_distance(p1, p2, by_element = TRUE) |> units::drop_units()
    # match_table |> add_column(distance = st_distance(p1, p2, by_element = TRUE) |> units::drop_units())
    match_table |> mutate(
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


analyze_matches <- function(match_table, table.x, table.y, meta.x, meta.y, climats = FALSE, years_threshold = 10L) {
    ymonthly <- function(table) {
        table |>
            index_by(ymt = ~ yearmonth(.)) |>
            summarise(across(!where(is.Date), ~ mean(., na.rm = TRUE)))
    }

    table.x.ymonthly <- ymonthly(table.x)
    table.y.ymonthly <- ymonthly(table.y)

    # matches <- matches |>
    #     semi_join(
    #         table.y |> as_tibble() |> select(!where(is.Date)) |> colnames() |> as_tibble_col("identifier.y"),
    #         by = "identifier.y"
    #     )
    if (climats) {
        climat <- function(table) {
            table |>
                index_by(month = ~ month(.)) |>
                summarise(across(everything(), ~ mean(., na.rm = TRUE)))
        }
        table.x.climat <- climat(table.x.ymonthly)
        table.y.climat <- climat(table.y.ymonthly)
        matches <- match_table |>
            rowwise() |>
            mutate(
                climatcorT = cor(pull(table.x.climat, identifier.x), pull(table.y.climat, identifier.y), use = "na.or.complete"),
            )
    }

    ### METADATA
    match_table <- three_way_join(meta.x, match_table, meta.y) |>
        collect() |>
        add_distance(meta.x, meta.y)

    match_table |>
        mutate(
            delH = abs(elevation.x - elevation.y),
            delZ = abs(dem.x - dem.y),
            strSym = stringsim(normalize_name(station_name.x), normalize_name(station_name.y), method = "jw")
        ) |>
        rowwise() |>
        mutate(
            Tinfo = Tinfo.numeric(pull(table.x, series_id.x), pull(table.y, series_id.y)),
            monthlyslopeT = slope_diff(pull(table.x.ymonthly, series_id.x), pull(table.y.ymonthly, series_id.y)),
            monthlydelT = mean(pull(table.x.ymonthly, series_id.x) - pull(table.y.ymonthly, series_id.y), na.rm = TRUE),
            monthlymae = mean(abs(pull(table.x.ymonthly, series_id.x) - pull(table.y.ymonthly, series_id.y)), na.rm = TRUE),
            monthlysdT = sd(pull(table.x.ymonthly, series_id.x) - pull(table.y.ymonthly, series_id.y), na.rm = TRUE),
            climat_availability = is_climatology_computable.series(
                pull(table.x, series_id.x),
                pull(table.y, series_id.y), table.x$date,
                n_years_minimum = years_threshold
            ) |> as_tibble() |> summarise(all_filter = all(clim_available), any_filter = any(clim_available))
        ) |>
        unnest(c(Tinfo, climat_availability))
}

compute_diffs <- function(matches, table.x, table.y) {
    matches |>
        select(starts_with("identifier")) |>
        rowwise() |>
        reframe(
            date = pull(table.x, date), value.x = pull(table.x, identifier.x), value.y = pull(table.y, identifier.y), diffs = value.x - value.y, identifier.x = identifier.x, identifier.y = identifier.y
        )
}
