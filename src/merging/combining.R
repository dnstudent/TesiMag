library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(stringi, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)
library(igraph, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(logger, warn.conflicts = FALSE)

source("src/database/write.R")
source("src/merging/pairing.R")

annual_index <- function(date) {
    4 * pi * (yday(date) / yday(make_date(year(date), 12L, 31L)) + 1 / 3)
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
        anti_join(tibble(key = already_there), by = "key")

    add_vertices(graph, nrow(missing), name = missing$key)
}

series_groups <- function(series_matches, metadata, data, tag) {
    graph_edgelist <- series_matches |>
        arrange(key_x, key_y, variable) |>
        select(key_x, key_y, {{ tag }})

    graph <- graph_from_isedge(
        graph_edgelist,
        {{ tag }},
        FALSE
    ) |>
        add_nonmatching(metadata)

    table <- group_by_component(graph) |>
        cross_join(tibble(variable = c(-1L, 1L))) |>
        semi_join(data |> distinct(key, variable) |> collect(), by = c("key", "variable"))
    list(
        "table" = table,
        "graph" = graph
    )
}

set_dataset_priority <- function(metadata, dataset_priority) {
    metadata |> mutate(dataset = factor(dataset, levels = rev(dataset_priority), ordered = TRUE))
}

diff_coeffs <- function(delmonthlyT, t) {
    n_data <- length(delmonthlyT |> na.omit())
    coeffs <- if (n_data >= 8) {
        coeffs <- lm(delmonthlyT ~ sin(t / 2) + sin(t) + sin(2 * t))$coefficients
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
        group_by(gkey, variable) |>
        mutate(priority = -row_number()) |>
        ungroup() |>
        select(gkey, variable, key, priority)
}

pairs_corrections <- function(pairs_list, data) {
    have_common_data <- pair_common_series(data, pairs_list, copy = TRUE, by = c("pkey", "key_x", "key_y", "variable")) |>
        mutate(delT = value_x - value_y) |>
        filter(abs(delT) < 10) |>
        group_by(pkey, month = month(date)) |>
        filter(n() > 25L) |>
        summarise(monthlydelT = mean(delT, na.rm = TRUE), .groups = "drop_last") |>
        mutate(t = 4 * pi * ((month - 0.5) / 12 + 1 / 3)) |>
        collect() |>
        summarise(coeffs = diff_coeffs(monthlydelT, t), .groups = "drop") |>
        unnest(coeffs)

    no_common_data <- pairs_list |>
        anti_join(have_common_data, by = "pkey") |>
        mutate(k0 = 0, k1 = 0, k2 = 0, k3 = 0) |>
        select(all_of(colnames(have_common_data)))

    rows_append(have_common_data, no_common_data)
}

make_pair_list <- function(prioritized_series_groups, optimal_offsets) {
    pl <- prioritized_series_groups |>
        group_by(gkey, variable) |>
        slice_max(priority, n = 2L, with_ties = FALSE) |>
        filter(n() == 2L) |>
        mutate(which_key = case_match(row_number(priority), 2L ~ "key_x", 1L ~ "key_y")) |>
        ungroup() |>
        pivot_wider(id_cols = c(gkey, variable), names_from = which_key, values_from = key) |>
        mutate(pkey = row_number())
    if (nrow(pl) > 0L) {
        pl |>
            left_join(optimal_offsets, by = c("key_x", "key_y", "variable")) |>
            mutate(offset_days = coalesce(offset_days, 0L))
    } else {
        pl |> mutate(key_x = 0L, key_y = 0L, offset_days = 0L)
    }
}

pairs_merge <- function(pairs_list, data, rejection_threshold) {
    corrections <- pairs_corrections(pairs_list, data)
    accepted_corrections <- corrections |> filter(abs(k0) <= rejection_threshold)
    rejected_corrections <- corrections |>
        filter(abs(k0) > rejection_threshold) |>
        left_join(pairs_list, by = "pkey", suffix = c("_x", "_y"))
    reference <- data |>
        inner_join(pairs_list |> select(pkey, key = key_x, variable), by = c("key", "variable"), copy = TRUE)
    corrected <- data |>
        inner_join(pairs_list |> select(pkey, key = key_y, variable), by = c("key", "variable"), copy = TRUE) |>
        anti_join(reference, by = c("pkey", "variable", "date")) |> #
        inner_join(accepted_corrections, by = "pkey", copy = TRUE) |>
        mutate(t = 4 * pi * (yday(date) / yday(make_date(year(date), 12L, 31L)) + 1 / 3)) |>
        mutate(correction = k0 + sin(t / 2) * k1 + sin(t) * k2 + sin(2 * t) * k3, value = value + correction) |>
        select(all_of(colnames(reference)))
    list("merged" = rows_append(reference, corrected) |> select(pkey, variable, date, value, from_key, correction), "rejected" = rejected_corrections)
}

prepare_data <- function(data, series_groups) {
    data |>
        semi_join(series_groups, by = c("key", "variable"), copy = TRUE) |>
        collect() |>
        mutate(from_key = key) |>
        select(date, variable, value, key, from_key) |>
        mutate(correction = 0)
}

prepare_offsets <- function(matches_offsets) {
    rows_append(
        matches_offsets |> select(key_x, key_y, variable, offset_days),
        matches_offsets |> select(key_x = key_y, key_y = key_x, variable, offset_days) |> mutate(offset_days = -offset_days)
    )
}

next_tables <- function(series_groups, current_pairs_list, current_merge_result, data, optimal_offsets) {
    series_groups <- series_groups |> anti_join(current_pairs_list, by = c("key" = "key_y", "variable"))

    next_pair_list <- series_groups |>
        make_pair_list(optimal_offsets)

    merge_data <- current_merge_result$merged |>
        left_join(current_pairs_list |> select(pkey, key = key_x), by = "pkey") |>
        select(-pkey)

    next_data <- data |>
        anti_join(current_pairs_list, by = c("key" = "key_x", "variable")) |>
        anti_join(current_pairs_list, by = c("key" = "key_y", "variable")) |>
        rows_append(merge_data)

    list(next_pair_list, next_data, series_groups)
}

dynamic_merge <- function(data, series_groups, metadata, optimal_offsets, rejection_threshold) {
    if (series_groups |> group_by(key, variable) |> count() |> filter(n > 1) |> nrow() > 0L) {
        stop("There are series with the same key and variable in the series_groups table")
    }
    key_meta <- series_groups |>
        left_join(metadata |> select(key, sensor_key, dataset), by = "key") |>
        arrange(desc(priority)) |>
        group_by(gkey, variable) |>
        summarise(from_keys = list(key), from_sensor_keys = list(sensor_key), from_datasets = list(dataset), key = first(key), .groups = "drop")
    data <- prepare_data(data, series_groups)
    optimal_offsets <- prepare_offsets(optimal_offsets)
    pair_list <- make_pair_list(series_groups, optimal_offsets)
    while (nrow(pair_list) > 0L) {
        merge_results <- pairs_merge(pair_list, data, rejection_threshold)
        # pair_list <- pair_list |> anti_join(merge_results$rejected, by = "pkey")
        r <- next_tables(series_groups, pair_list, merge_results, data, optimal_offsets)
        pair_list <- r[[1]]
        data <- r[[2]]
        series_groups <- r[[3]]
    }
    list("meta" = key_meta, "data" = data)
}

incompatible_merges <- function(data, series_groups, optimal_offsets, cycles, rejection_threshold) {
    if (series_groups |> group_by(key, variable) |> count() |> filter(n > 1) |> nrow() > 0L) {
        stop("There are series with the same key and variable in the series_groups table")
    }
    data <- prepare_data(data, series_groups)
    optimal_offsets <- prepare_offsets(optimal_offsets)
    pair_list <- make_pair_list(series_groups, optimal_offsets)
    n_rejected <- 0L
    counter <- 0L
    rejects <- tibble(pkey = integer(), k0 = double(), k1 = double(), k2 = double(), k3 = double(), gkey = integer(), key_x = integer(), key_y = integer(), variable = integer(), offset_days = integer())
    while ((n_rejected == 0L | counter < cycles) & nrow(pair_list) > 0L) {
        merge_results <- pairs_merge(pair_list, data, rejection_threshold)
        n_rejected <- nrow(merge_results$rejected)
        rejects <- rows_append(rejects, merge_results$rejected)
        r <- next_tables(series_groups, pair_list, merge_results, data, optimal_offsets)
        pair_list <- r[[1]]
        data <- r[[2]]
        series_groups <- r[[3]]
        counter <- counter + 1L
    }
    rejects
}
