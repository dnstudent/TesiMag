library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(stringi, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)
library(igraph, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/database/write.R")
source("src/merging/pairing.R")

annual_index <- function(date) {
    2 * pi * yday(date) / yday(ceiling_date(date, unit = "year") - as.difftime(1, units = "days"))
}

#' Computes a diffs table for a given match table, loading the data series from the given tables.
#'
#' @param match_list A match table, as returned by \code{\link{match_table}}, complete with two series identifer columns and a match_id column.
#' @param data_table A table containing the data series for the first database, one per column, and a date index.
#' @return A tsibble with a column for each match_id, containing the difference between the two series linked to the id.
diffs_table <- function(match_list, data_table) {
    match_list |>
        select(variable, station_id.x, station_id.y, match_id) |>
        rowwise() |>
        reframe(
            diffs = data_table |> pull(paste0(variable, "_", station_id.x)) - data_table |> pull(paste0(variable, "_", station_id.y)),
            date = data_table$date,
            match_id = match_id
        ) |>
        pivot_wider(id_cols = date, values_from = diffs, names_from = match_id) |>
        arrange(date) |>
        as_tsibble(index = date) |>
        fill_gaps(.full = TRUE)
}

#' Computes monthly corrections for a given diffs table
#'
#' @param diffs_table A daily diffs table, consisting of a tsibble with a column for each match_id
#' @return A tibble with a column for each match_id, containing the monthly correction
monthly_corrections <- function(diffs_table) {
    diffs_table |>
        index_by(ymt = ~ yearmonth(.)) |>
        summarise(across(everything(), ~ mean(., na.rm = TRUE))) |>
        index_by(mt = ~ month(.)) |>
        summarise(across(everything(), ~ mean(., na.rm = TRUE))) |>
        mutate(t = annual_index(date)) |>
        as_tibble() |>
        select(-ymt, -mt)
}

prepare_for_modeling <- function(monthly_corrections) {
    monthly_corrections |>
        pivot_longer(cols = !c(date, t), names_to = "match_id", values_to = "diffs") |>
        arrange(match_id, t)
}

model_and_predict_corrections <- function(monthly_diffs, t, orig_t) {
    n_data <- length(monthly_diffs |> na.omit())
    if (n_data >= 8) {
        model <- lm(monthly_diffs ~ sin(t / 2) + sin(t) + sin(3 * t / 2))
        predict(model, newdata = orig_t) |> unname()
    } else if (n_data > 2) {
        rep(mean(monthly_diffs, na.rm = TRUE), times = nrow(orig_t))
    } else {
        rep(0, times = nrow(orig_t))
    }
}

coalesce_group <- function(id.x, ids.y, match_ids, data_table, corrections) {
    if (!is.null(corrections)) {
        replacement_values <- select(data_table, all_of(ids.y)) + select(corrections, all_of(match_ids))
    } else {
        replacement_values <- select(data_table, all_of(ids.y))
    }
    coalesce(pull(data_table, id.x), !!!replacement_values)
}

prepare_corrections_ <- function(analyzed_matches, data_table, .test_bounds, ...) {
    corrections <- diffs_table(analyzed_matches, data_table) |>
        monthly_corrections() |>
        reframe(
            across(!c(date, t), ~ model_and_predict_corrections(., t, data_table)),
            t = data_table$t
        )
    if (!is.null(.test_bounds)) {
        # Asserting that the corrections are reasonable in absolute value
        corrections |>
            assertr::assert(
                assertr::within_bounds(-.test_bounds, .test_bounds),
                -t
            )
    }
    corrections
}

T_merge_left_if <- expr(
    (valid_days_inters > 360) &
        ((variable == "T_MIN" & monthlydelT >= 0) | (variable == "T_MAX" & monthlydelT <= 0))
)

days_merge_left_if <- expr(
    (valid_days_inters <= 360) & (valid_days.x >= valid_days.y)
)

default_symmetric_filter <- function(analysis) {
    # if (("priority.x" %in% colnames(analysis)) && ("priority.y" %in% colnames(analysis))) {
    analysis |> filter(
        (priority.x < priority.y) |
            ((priority.x == priority.y) &
                (valid_days.x > valid_days.y |
                    (valid_days.x == valid_days.y & station_id.x > station_id.y)
                )
            )
    )
    # } else {
    #     analysis |> filter(!!T_merge_left_if | !!days_merge_left_if)
    # }
}

merge_database_by <- function(analyzed_matches, data_table, .test_bounds, ...) {
    data_table <- fill_gaps(data_table) |>
        as_tibble() |>
        mutate(t = annual_index(date)) |>
        arrange(date)

    #  Computing a correction table for each match
    corrections <- prepare_corrections_(analyzed_matches, data_table, .test_bounds)

    analyzed_matches |>
        arrange(station_id.x, desc(f0), abs(monthlydelT), desc(valid_days_inters), ...) |>
        group_by(variable, station_id.x) |>
        reframe(
            value = coalesce_group(paste0(first(variable), "_", first(station_id.x)), paste0(variable, "_", station_id.y), match_id, data_table, corrections),
            date = data_table$date,
            merged = !(is.na(value) | !is.na(pull(data_table, paste0(first(variable), "_", first(station_id.x))))),
        ) |>
        drop_na(value) |>
        rename(station_id = station_id.x)
}

filter_remaining_ <- function(target_list, control_list, match_list) {
    target_list |>
        anti_join(match_list, by = join_by(station_id == station_id.x)) |>
        anti_join(match_list, by = join_by(station_id == station_id.y)) |>
        semi_join(control_list, by = "station_id")
}

#' Combines the data and metadata of merged and not merged series.
#'
#' Replaces the data and metadata of the ancestors of the merged series with the merged data and metadata.
#'
#' @param merged_data A dataframe/Table containing the merged series data.
#' @param whole_database A database containing the data and metadata of the whole database.
#' @param approved_match_list A match table, as returned by \code{\link{match_table}}, complete with two `station_id` and a `variable` columns.
#' @param check_consistency Whether to check the consistency of the resulting database.
#'
#' @return A database containing the data and metadata of the merged dataset.
concat_merged_and_unmerged <- function(merged_data, whole_database, approved_match_list, check_consistency = TRUE) {
    # Only station metadata from the left (.x) database is kept for merged series.
    merged_meta <- whole_database$meta |>
        semi_join(approved_match_list, join_by(station_id == station_id.x))

    # Must pay attention to properly manage the "variable"-"station_id" key.
    nonmerged_data <- whole_database$data |>
        anti_join(approved_match_list, join_by(station_id == station_id.x, variable)) |>
        anti_join(approved_match_list, join_by(station_id == station_id.y, variable))

    concat_data <- concat_tables(merged_data |> compute(), nonmerged_data |> compute(), unify_schemas = FALSE)
    concat_meta <- whole_database$meta |> semi_join(concat_data, by = "station_id")

    if (check_consistency) {
        if (concat_data |> collect() |> duplicates(key = c(station_id, variable), index = date) |> nrow() > 0) {
            stop("Duplicates were found in the concatenated data")
        }
        concat_meta |>
            collect() |>
            assert(is_uniq, station_id)
    }

    as_database.ArrowTabular(concat_meta, concat_data)
}

vertices_groups <- function(graph) {
    mmb <- components(graph, mode = "weak")$membership
    tibble(id = as.integer(names(mmb)), gid = as.integer(unname(mmb)))
}

graph_from_isedge <- function(matches, is_edge_variable, directed) {
    matches |>
        filter({{ is_edge_variable }}) |>
        select(id_x, id_y) |>
        mutate(across(everything(), as.character)) |>
        as.matrix() |>
        graph_from_edgelist(directed = directed)
}

add_nonmatching <- function(graph, metadata) {
    already_there <- V(graph) |>
        names() |>
        as.integer()

    missing <- metadata |>
        distinct(id) |>
        collect() |>
        anti_join(tibble(id = already_there), by = "id")

    add_vertices(graph, nrow(missing), name = missing$id)
}


group_series_matches <- function(series_matches, metadata, tag, vertex_grouping, direct) {
    graph_edgelist <- series_matches |>
        arrange(id_x, id_y, variable) |>
        select(id_x, id_y, variable, {{ tag }})

    graph_tmin <- graph_from_isedge(
        graph_edgelist |> filter(variable == -1L),
        {{ tag }},
        direct
    ) |>
        add_nonmatching(metadata)

    graph_tmax <- graph_from_isedge(
        graph_edgelist |> filter(variable == 1L),
        {{ tag }},
        direct
    ) |>
        add_nonmatching(metadata)

    list(
        "table" = bind_rows(vertex_grouping(graph_tmin) |> mutate(variable = -1L), vertex_grouping(graph_tmax) |> mutate(variable = 1L)),
        "graph_tmin" = graph_tmin,
        "graph_tmax" = graph_tmax
    )
}

set_dataset_priority <- function(metadata, dataset_priority) {
    metadata |> mutate(dataset = factor(dataset, levels = dataset_priority, ordered = TRUE))
}

diff_coeffs <- function(delmonthlyT, t) {
    n_data <- length(delmonthlyT |> na.omit())
    coeffs <- if (n_data >= 8) {
        coeffs <- lm(delmonthlyT ~ sin(t / 2) + sin(t) + sin(3 * t / 2))$coefficients
        names(coeffs) <- c("k0", "k1", "k2", "k3")
        coeffs
    } else if (n_data > 2) {
        c("k0" = mean(delmonthlyT, na.rm = TRUE), "k1" = 0, "k2" = 0, "k3" = 0)
    } else {
        c("k0" = 0, "k1" = 0, "k2" = 0, "k3" = 0)
    }
    as_tibble_row(coeffs)
}

rank_inside_group <- function(series_groups, metadata, dataset_priority, ...) {
    series_groups |>
        left_join(metadata |> collect(), by = "id", copy = TRUE) |>
        set_dataset_priority(dataset_priority) |>
        arrange(...) |>
        group_by(gid, variable) |>
        mutate(priority = -row_number()) |>
        ungroup() |>
        select(gid, variable, id, priority)
}

ranked_match_table <- function(ranked_series_groups, optimal_offset) {
    all_offsets <- bind_rows(
        select(optimal_offset, id_x, id_y, variable, offset_days),
        select(optimal_offset, id_x = id_y, id_y = id_x, variable, offset_days)
    )

    ranked_series_groups |>
        group_by(gid, variable) |>
        filter(n() > 1L) |>
        slice_max(priority, with_ties = FALSE) |>
        ungroup() |>
        select(gid, id, variable) |>
        left_join(ranked_series_groups, by = c("gid", "variable"), suffix = c("_x", "_y")) |>
        filter(id_x != id_y) |>
        left_join(all_offsets, by = c("id_x", "id_y", "variable"), relationship = "many-to-one")
}

#' Computes the coefficients for the model Ty - Tx ~ k0 + k1 * sin(t / 2) + k2 * sin(t) + k3 * sin(3 * t / 2)
compute_corrections <- function(lagged_series_matches, data) {
    lsm <- copy_to(data$src$con, lagged_series_matches, overwrite = TRUE, name = "lagged_series_matches_tmp")
    corrections <- pair_full_series(data, lsm, by = c("id_x", "id_y", "variable")) |>
        mutate(delT = value_y - value_x) |>
        group_by(variable, id_x, id_y, month = month(date), year = year(date)) |>
        summarise(delT = mean(delT, na.rm = TRUE), .groups = "drop_last") |>
        summarise(monthlydelT = mean(delT, na.rm = TRUE), .groups = "drop") |>
        # summarise(across(starts_with("value"), ~ mean(., na.rm = TRUE)), .groups = "drop_last") |>
        # summarise(across(starts_with("value"), ~ mean(., na.rm = TRUE)), .groups = "drop_last") |>
        # mutate(delmonthlyT = value_y - value_x, t = 2 * pi * (month - 0.5) / 12) |>
        mutate(t = 2 * pi * (month - 0.5) / 12) |>
        collect() |>
        group_by(variable, id_x, id_y) |>
        summarise(coeffs = diff_coeffs(monthlydelT, t), .groups = "drop") |>
        unnest(coeffs) |>
        left_join(lagged_series_matches |> select(gid, id_x, id_y, variable), by = c("id_x", "id_y", "variable"))

    dbExecute(data$src$con, "DROP TABLE lagged_series_matches_tmp")
    corrections
}

existing_measures <- function(series, data) {
    data |>
        semi_join(series, by = c("station_id", "variable"), copy = TRUE) |>
        filter(!is.na(value)) |>
        select(station_id, variable, date)
}

#' Merges the series in a given group to produce a single series.
#'
#' @param series_groups A table of series groups, consisting of id (station), variable, and gid.
#' ... specifies the order in which the series are merged comparing series metadata: the first series is the one with the highest priority, and so on.
merge_series_group <- function(series_groups, metadata, data, optimal_offset, dataset_priority, ...) {
    # 1. Ranking the series inside their group according to the criteria given in ..., which is passed to arrange()
    ranked_series_groups <- rank_inside_group(series_groups, metadata, dataset_priority, ...)
    # ranked_series_groups <- copy_to(data$src$con, ranked_series_groups, overwrite = TRUE, name = "ranked_series_matches_tmp")

    # 2. Building the match table which describes how series are merged. Every series gets matched against the highest ranked series in the group.
    merging_matches <- ranked_match_table(ranked_series_groups, optimal_offset)
    merged_on_series <- merging_matches |>
        distinct(gid, station_id = id_x, variable)

    # 3. Computing the corrections for each series in the group
    corrections <- compute_corrections(merging_matches, data)
    # `corrections` is a table with columns gid, variable, id_x, id_y, k0, k1, k2, k3

    # 4. Listing the dates that are already present in the highest ranked series for each group
    already_there <- existing_measures(merged_on_series, data) |> compute()

    # 5. Corrected series. These are the id_y series, corrected by the coefficients computed in step 3.
    integrations <- data |>
        inner_join(corrections, by = c("variable", "station_id" = "id_y"), copy = TRUE) |>
        # table with columns: gid, variable, id_x, station_id, date, value, k0, k1, k2, k3
        # Removing the measures that are already present in the highest ranked series for each group, stored in `already_there`
        anti_join(already_there, by = c("id_x" = "station_id", "variable", "date")) |>
        mutate(
            t = 2 * pi * yday(date) / yday(make_date(year(date), 1L, 1L)),
            delT = k0 + k1 * sin(t / 2.0) + k2 * sin(t) + k3 * sin(3.0 * t / 2.0),
            value = value - delT
        ) |>
        select(!c(k0, k1, k2, k3, t, delT, id_x))

    # 6. Using the corrected series to fill the gaps in the highest ranked series
    data |>
        inner_join(merged_on_series, by = c("variable", "station_id"), copy = TRUE) |>
        # table with columns: gid, variable, station_id, date, value
        rows_append(integrations) |>
        left_join(ranked_series_groups, by = c("gid", "variable", "station_id" = "id"), copy = TRUE) |>
        group_by(gid, variable, date) |>
        window_order(priority) |>
        summarise(value = first(value), station_id = first(station_id), .groups = "drop")
}
