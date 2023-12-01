library(stringr, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(tsibble, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)
library(digest, warn.conflicts = FALSE)
library(stringdist, warn.conflicts = FALSE)

source("src/load/load.R")

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

add_distances <- function(match_table, metadata.x, metadata.y) {
    bind_cols(
        match_table,
        distance = st_distance(
            left_join(match_table, metadata.x, join_by(identifier.x == identifier, variable), relationship = "many-to-one") |> st_as_sf(),
            left_join(match_table, metadata.y, join_by(identifier.y == identifier, variable), relationship = "many-to-one") |> st_as_sf(),
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


analyze_matches <- function(matches, table.x, table.y, climats = FALSE, years_threshold = 10L) {
    table.x.ymonthly <- table.x |>
        index_by(ymt = ~ yearmonth(.)) |>
        summarise(across(!where(is.Date), ~ mean(., na.rm = TRUE)))
    table.y.ymonthly <- table.y |>
        index_by(ymt = ~ yearmonth(.)) |>
        summarise(across(!where(is.Date), ~ mean(., na.rm = TRUE)))

    matches <- matches |>
        semi_join(
            table.y |> as_tibble() |> select(!where(is.Date)) |> colnames() |> as_tibble_col("identifier.y"),
            by = "identifier.y"
        )
    if (climats) {
        table.x.climat <- table.x.ymonthly |>
            index_by(month = ~ month(.)) |>
            summarise(across(everything(), ~ mean(., na.rm = TRUE)))
        table.y.climat <- table.y.ymonthly |>
            index_by(month = ~ month(.)) |>
            summarise(across(everything(), ~ mean(., na.rm = TRUE)))
        matches <- matches |>
            rowwise() |>
            mutate(
                climatcorT = cor(pull(table.x.climat, identifier.x), pull(table.y.climat, identifier.y), use = "na.or.complete"),
            )
    }
    matches |>
        mutate(
            delH = abs(elevation.x - elevation.y),
            delZ = abs(dem.x - dem.y),
            strSym = stringsim(normalize_name(anagrafica.x), normalize_name(anagrafica.y), method = "jw")
        ) |>
        rowwise() |>
        mutate(
            Tinfo = Tinfo.numeric(pull(table.x, identifier.x), pull(table.y, identifier.y)),
            monthlyslopeT = slope_diff(pull(table.x.ymonthly, identifier.x), pull(table.y.ymonthly, identifier.y)),
            monthlydelT = mean(pull(table.x.ymonthly, identifier.x) - pull(table.y.ymonthly, identifier.y), na.rm = TRUE),
            monthlymae = mean(abs(pull(table.x.ymonthly, identifier.x) - pull(table.y.ymonthly, identifier.y)), na.rm = TRUE),
            monthlysdT = sd(pull(table.x.ymonthly, identifier.x) - pull(table.y.ymonthly, identifier.y), na.rm = TRUE),
            climat_availability = is_climatology_computable.series(
                pull(table.x, identifier.x),
                pull(table.y, identifier.y), table.x$date,
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
