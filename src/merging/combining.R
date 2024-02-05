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

group_by_component <- function(graph) {
    mmb <- components(graph, mode = "weak")$membership
    tibble(key = as.integer(names(mmb)), gkey = as.integer(unname(mmb)))
}

graph_from_isedge <- function(matches, is_edge_variable, directed) {
    matches |>
        filter({{ is_edge_variable }}) |>
        select(key_x, key_y) |>
        mutate(across(everything(), as.character)) |>
        as.matrix() |>
        graph_from_edgelist(directed = directed)
}

add_nonmatching <- function(graph, metadata) {
    already_there <- V(graph) |>
        names() |>
        as.integer()

    missing <- metadata |>
        distinct(key) |>
        collect() |>
        anti_join(tibble(key = already_there), by = "key")

    add_vertices(graph, nrow(missing), name = missing$key)
}


series_groups <- function(series_matches, metadata, tag, vertex_grouping, direct) {
    graph_edgelist <- series_matches |>
        arrange(key_x, key_y, variable) |>
        select(key_x, key_y, {{ tag }})

    # graph_tmin <- graph_from_isedge(
    #     graph_edgelist |> filter(variable == -1L),
    #     {{ tag }},
    #     direct
    # ) |>
    #     add_nonmatching(metadata)

    # graph_tmax <- graph_from_isedge(
    #     graph_edgelist |> filter(variable == 1L),
    #     {{ tag }},
    #     direct
    # ) |>
    #     add_nonmatching(metadata)

    graph <- graph_from_isedge(
        graph_edgelist,
        {{ tag }},
        direct
    ) |>
        add_nonmatching(metadata)

    # table <- bind_rows(vertex_grouping(graph_tmin) |> mutate(variable = -1L), vertex_grouping(graph_tmax) |> mutate(variable = 1L))
    table <- vertex_grouping(graph)
    list(
        "table" = table,
        # "graph_tmin" = graph_tmin,
        # "graph_tmax" = graph_tmax
        "graph" = graph
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

rank_series_groups <- function(series_groups, metadata, dataset_priority, ...) {
    series_groups |>
        left_join(metadata, by = "key") |>
        set_dataset_priority(dataset_priority) |>
        arrange(...) |>
        group_by(gkey) |>
        mutate(priority = -row_number()) |>
        ungroup() |>
        select(gkey, key, priority)
}

match_with_highest_ranked <- function(ranked_series_groups, match_offsets) {
    all_offsets <- bind_rows(
        match_offsets |> select(key_x, key_y, variable, offset_days),
        match_offsets |> select(key_x = key_y, key_y = key_x, variable, offset_days)
    )

    ranked_series_groups |>
        group_by(gid, variable) |>
        filter(n() > 1L) |>
        slice_max(priority, with_ties = FALSE) |>
        ungroup() |>
        select(gid, id, variable) |>
        left_join(ranked_series_groups, by = c("gid", "variable"), suffix = c("_x", "_y")) |>
        filter(key_x != key_y) |>
        left_join(all_offsets, by = c("key_x", "key_y", "variable"), relationship = "many-to-one")
}

#' Computes the coefficients for the model Ty - Tx ~ k0 + k1 * sin(t / 2) + k2 * sin(t) + k3 * sin(3 * t / 2)
compute_corrections <- function(lagged_series_matches, data) {
    lsm <- copy_to(data$src$con, lagged_series_matches, overwrite = TRUE, name = "lagged_series_matches_tmp")
    # Using pair_full_series instead of pair_common_series as to preserve the presence of matching series with no common period
    corrections <- pair_full_series(data, lsm, by = c("key_x", "key_y", "variable")) |>
        mutate(delT = value_y - value_x) |>
        group_by(variable, key_x, key_y, month = month(date), year = year(date)) |>
        summarise(delT = mean(delT, na.rm = TRUE), .groups = "drop_last") |>
        summarise(monthlydelT = mean(delT, na.rm = TRUE), .groups = "drop") |>
        # summarise(across(starts_with("value"), ~ mean(., na.rm = TRUE)), .groups = "drop_last") |>
        # summarise(across(starts_with("value"), ~ mean(., na.rm = TRUE)), .groups = "drop_last") |>
        # mutate(delmonthlyT = value_y - value_x, t = 2 * pi * (month - 0.5) / 12) |>
        mutate(t = 2 * pi * (month - 0.5) / 12) |>
        collect() |>
        group_by(variable, key_x, key_y) |>
        summarise(coeffs = diff_coeffs(monthlydelT, t), .groups = "drop") |>
        unnest(coeffs) |>
        left_join(lagged_series_matches |> select(gid, key_x, key_y, variable), by = c("key_x", "key_y", "variable"))

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
    merging_matches <- match_with_highest_ranked(ranked_series_groups, optimal_offset)
    merged_on_series <- merging_matches |>
        distinct(gid, station_id = key_x, variable)

    # 3. Computing the corrections for each series in the group
    corrections <- compute_corrections(merging_matches, data)
    # `corrections` is a table with columns gid, variable, key_x, key_y, k0, k1, k2, k3

    # 4. Listing the dates that are already present in the highest ranked series for each group
    already_there <- existing_measures(merged_on_series, data) |> compute()

    # 5. Corrected series. These are the key_y series, corrected by the coefficients computed in step 3.
    integrations <- data |>
        inner_join(corrections, by = c("variable", "station_id" = "key_y"), copy = TRUE) |>
        # table with columns: gid, variable, key_x, station_id, date, value, k0, k1, k2, k3
        # Removing the measures that are already present in the highest ranked series for each group, stored in `already_there`
        anti_join(already_there, by = c("key_x" = "station_id", "variable", "date")) |>
        mutate(
            t = 2 * pi * yday(date) / yday(make_date(year(date), 1L, 1L)),
            delT = k0 + k1 * sin(t / 2.0) + k2 * sin(t) + k3 * sin(3.0 * t / 2.0),
            value = value - delT
        ) |>
        select(!c(k0, k1, k2, k3, t, delT, key_x))

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

merge_series_groups.2 <- function(series_groups, metadata, data, dataset_priority, ...) {
    ranked_series_groups <- rank_series_groups(series_groups, metadata, dataset_priority, ...)
    ranked_series_groups <- copy_to(data$src$con, ranked_series_groups, overwrite = TRUE, name = "ranked_series_matches_tmp")
    ranked_data <- ranked_series_groups |> left_join(data, by = "key")
    ranked_data |>
        # arrange(priority) |>
        group_by(gkey, variable, date) |>
        window_order(desc(priority)) |>
        summarise(value = first(value), from_dataset = first(dataset), from_sensor = first(sensor_key), from_key = first(key), .groups = "drop")
}
