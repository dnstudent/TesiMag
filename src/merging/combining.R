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
    2 * pi * (yday(date) / yday(make_date(year(date), 12L, 31L)))
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

sin_coeffs <- function(delmonthlyT, t) {
    coeffs <- lm(delmonthlyT ~ sin(t / 2) + sin(t) + sin(2 * t))$coefficients
    names(coeffs) <- c("k0", "k1", "k2", "k3")
    as_tibble_row(coeffs)
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

pairs_corrections <- function(pairs_list, data, ignore_corrections = NULL) {
    if (!is.null(ignore_corrections)) {
        pl <- pairs_list |> anti_join(ignore_corrections, by = colnames(ignore_corrections))
    } else {
        pl <- pairs_list
    }

    DELT <- pair_common_series(data, pl, copy = TRUE, by = c("pkey", "key_x", "key_y", "variable")) |>
        mutate(delT = value_x - value_y, month = month(date)) |>
        filter(abs(delT) < 7, !is.na(delT)) |>
        compute()

    sin_correction_keys <- DELT |>
        group_by(pkey, month) |>
        tally() |>
        filter(n >= 20L) |>
        ungroup(month) |>
        tally() |>
        filter(n >= 8L) |>
        ungroup()

    sin_corrections <- DELT |>
        semi_join(sin_correction_keys, by = "pkey") |>
        group_by(pkey, month) |>
        filter(n() >= 20L) |>
        summarise(monthlydelT = mean(delT, na.rm = TRUE), .groups = "drop_last") |>
        mutate(t = 2 * pi * (month - 0.5) / 12) |>
        collect() |>
        group_by(pkey) |>
        summarise(coeffs = sin_coeffs(monthlydelT, t), .groups = "drop") |>
        unnest(coeffs)

    mean_correction_keys <- DELT |>
        anti_join(sin_corrections, by = "pkey") |>
        distinct(pkey, month) |>
        group_by(pkey) |>
        filter(n() > 2L) |>
        ungroup()

    mean_corrections <- DELT |>
        semi_join(mean_correction_keys, by = "pkey") |>
        group_by(pkey) |>
        summarise(k0 = mean(delT, na.rm = TRUE), k1 = 0, k2 = 0, k3 = 0, .groups = "drop") |>
        collect()

    corrections <- rows_append(sin_corrections, mean_corrections)

    zero_corrections <- pairs_list |>
        anti_join(corrections, by = "pkey") |>
        mutate(k0 = 0, k1 = 0, k2 = 0, k3 = 0) |>
        select(all_of(colnames(corrections)))

    rows_append(corrections, zero_corrections)
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

pairs_merge <- function(pairs_list, data, rejection_threshold, ignore_corrections = NULL) {
    corrections <- pairs_corrections(pairs_list, data, ignore_corrections)
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
        mutate(t = 2 * pi * (yday(date) / yday(make_date(year(date), 12L, 31L)))) |>
        mutate(correction = k0 + sin(t / 2) * k1 + sin(t) * k2 + sin(2 * t) * k3, value = value + correction) |>
        select(all_of(colnames(reference)))
    list("merged" = rows_append(reference, corrected) |> select(pkey, variable, date, value, from_key, correction), "rejected" = rejected_corrections, "accepted" = accepted_corrections)
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

next_tables <- function(series_groups, current_pairs_list, current_merge_result, data, optimal_offsets, running_k) {
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

    coeffs_table <- current_merge_result$accepted |>
        left_join(current_pairs_list, by = "pkey") |>
        select(-pkey)
    if (!is.null(running_k)) {
        running_k <- bind_rows(running_k, coeffs_table)
    } else {
        running_k <- coeffs_table
    }

    list(next_pair_list, next_data, series_groups, running_k)
}

make_exclusion_table <- function(matches, exclusion) {
    m <- matches |>
        semi_join(exclusion, by = colnames(exclusion)) |>
        select(key_x, key_y, variable)
    bind_rows(m, m |> select(key_x = key_y, key_y = key_x, variable))
}

#' Merges the time series in data according to the series_groups table.
dynamic_merge <- function(data, series_groups, metadata, optimal_offsets, rejection_threshold, ignore_corrections = NULL) {
    if (series_groups |> group_by(key, variable) |> count() |> filter(n > 1L) |> nrow() > 0L) {
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
    running_ks <- NULL
    while (nrow(pair_list) > 0L) {
        merge_results <- pairs_merge(pair_list, data, rejection_threshold, ignore_corrections)
        r <- next_tables(series_groups, pair_list, merge_results, data, optimal_offsets, running_ks)
        pair_list <- r[[1]]
        data <- r[[2]]
        series_groups <- r[[3]]
        running_ks <- r[[4]]
    }
    list("meta" = key_meta, "data" = data, "coeffs" = running_ks)
}

incompatible_merges <- function(data, series_groups, optimal_offsets, cycles, rejection_threshold, ignore_corrections = NULL) {
    if (series_groups |> group_by(key, variable) |> count() |> filter(n > 1) |> nrow() > 0L) {
        stop("There are series with the same key and variable in the series_groups table")
    }
    data <- prepare_data(data, series_groups)
    optimal_offsets <- prepare_offsets(optimal_offsets)
    pair_list <- make_pair_list(series_groups, optimal_offsets)
    n_rejected <- 0L
    counter <- 0L
    rejects <- tibble(pkey = integer(), k0 = double(), k1 = double(), k2 = double(), k3 = double(), gkey = integer(), key_x = integer(), key_y = integer(), variable = integer(), offset_days = integer())
    running_ks <- NULL
    while ((n_rejected == 0L | counter < cycles) & nrow(pair_list) > 0L) {
        merge_results <- pairs_merge(pair_list, data, rejection_threshold, ignore_corrections)
        n_rejected <- nrow(merge_results$rejected)
        rejects <- rows_append(rejects, merge_results$rejected)
        r <- next_tables(series_groups, pair_list, merge_results, data, optimal_offsets, running_ks)
        pair_list <- r[[1]]
        data <- r[[2]]
        series_groups <- r[[3]]
        running_ks <- r[[4]]
        counter <- counter + 1L
    }
    rejects
}
