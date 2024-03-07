library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(stringi, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)
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
    mmb <- igraph::components(graph, mode = "weak")$membership
    tibble(key = as.integer(names(mmb)), gkey = as.integer(unname(mmb)))
}

graph_from_isedge <- function(matches, is_edge_variable, directed) {
    matches |>
        filter({{ is_edge_variable }}) |>
        select(key_x, key_y) |>
        mutate(across(everything(), as.character)) |>
        as.matrix() |>
        igraph::graph_from_edgelist(directed = directed)
}

add_nonmatching <- function(graph, metadata) {
    already_there <- igraph::V(graph) |>
        names() |>
        as.integer()

    missing <- metadata |>
        distinct(key) |>
        anti_join(tibble(key = already_there), by = "key")

    igraph::add_vertices(graph, nrow(missing), name = missing$key)
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
    if (length(delmonthlyT > 0)) {
        coeffs <- lm(delmonthlyT ~ sin(t / 2) + sin(t) + sin(2 * t))$coefficients
        names(coeffs) <- c("k0", "k1", "k2", "k3")
        as_tibble_row(coeffs)
    } else {
        tibble(k0 = 0, k1 = 0, k2 = 0, k3 = 0)
    }
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

pairs_corrections <- function(pairs_list, data, ignore_corrections) {
    if (!is.null(ignore_corrections)) {
        pl <- pairs_list |> anti_join(ignore_corrections, by = colnames(ignore_corrections))
    } else {
        pl <- pairs_list
    }

    DELT <- pair_common_series(data, pl, copy = TRUE, by = c("pkey", "key_x", "key_y", "variable")) |>
        mutate(delT = value_x - value_y, month = month(date)) |>
        filter(abs(delT) < 7, !is.na(delT)) |>
        compute()

    available_months <- DELT |>
        group_by(pkey, month) |>
        tally() |>
        filter(n >= 15L) |>
        ungroup(month) |>
        tally() |>
        ungroup()

    sin_corrections <- DELT |>
        semi_join(available_months |> filter(n >= 8L), by = "pkey") |>
        mutate(t = 2 * pi * (yday(date) / yday(make_date(year(date), 12L, 31L)))) |>
        collect() |>
        group_by(pkey) |>
        summarise(coeffs = sin_coeffs(delT, t), .groups = "drop") |>
        unnest(coeffs)

    mean_corrections <- DELT |>
        semi_join(available_months |> filter(2L < n, n < 8L), by = "pkey") |>
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

pairs_merge <- function(pairs_list, data, rejection_threshold, ignore_corrections, contribution_threshold) {
    corrections <- pairs_corrections(pairs_list, data, ignore_corrections) |> left_join(pairs_list, by = "pkey")
    accepted_corrections <- corrections |> filter(abs(k0 + 2 * k1 / pi) <= rejection_threshold)
    rejected_corrections <- corrections |>
        filter(abs(k0 + 2 * k1 / pi) > rejection_threshold)
    master <- data |>
        inner_join(pairs_list |> select(pkey, key = key_x, variable), by = c("key", "variable"))

    integrations <- data |>
        inner_join(accepted_corrections |> select(pkey, key = key_y, variable, k0, k1, k2, k3, offset_days), by = c("key", "variable")) |>
        mutate(date = date + days(offset_days)) |>
        anti_join(master, by = c("pkey", "variable", "date")) |>
        group_by(pkey, variable) |>
        mutate(contrib = n()) |>
        ungroup() |>
        filter(contrib >= contribution_threshold) |>
        mutate(t = 2 * pi * (yday(date) / yday(make_date(year(date), 12L, 31L)))) |>
        mutate(correction = k0 + sin(t / 2) * k1 + sin(t) * k2 + sin(2 * t) * k3, value = value + correction) |>
        select(all_of(colnames(master)))
    list("merged" = rows_append(master, integrations) |> select(pkey, variable, date, value, from_key, correction), "rejected" = rejected_corrections, "accepted" = accepted_corrections)
}

prepare_data <- function(data, series_groups) {
    data |>
        semi_join(series_groups, by = c("key", "variable"), copy = TRUE) |>
        mutate(from_key = key) |>
        select(date, variable, value, key, from_key) |>
        mutate(correction = 0) |>
        collect()
}

prepare_offsets <- function(matches_offsets) {
    rows_append(
        matches_offsets |> select(key_x, key_y, variable, offset_days),
        matches_offsets |> select(key_x = key_y, key_y = key_x, variable, offset_days) |> mutate(offset_days = -offset_days)
    )
}

update_tables <- function(series_groups, current_pairs_list, current_merge_result, data, optimal_offsets, running_k, running_rejected) {
    series_groups <- series_groups |> anti_join(current_pairs_list, by = c("key" = "key_y", "variable"))

    next_pair_list <- series_groups |>
        make_pair_list(optimal_offsets)

    merged_data <- current_merge_result$merged |>
        left_join(current_pairs_list |> select(pkey, key = key_x), by = "pkey") |>
        select(-pkey)

    next_data <- data |>
        anti_join(current_pairs_list, by = c("key" = "key_x", "variable")) |>
        anti_join(current_pairs_list, by = c("key" = "key_y", "variable")) |>
        rows_append(merged_data)

    coeffs_table <- current_merge_result$accepted |>
        # left_join(current_pairs_list |> select(pkey, ), by = "pkey") |>
        select(-pkey)
    if (!is.null(running_k)) {
        running_k <- bind_rows(running_k, coeffs_table)
    } else {
        running_k <- coeffs_table
    }
    rejected_list <- current_merge_result$rejected |>
        select(key = key_y, variable)
    if (!is.null(running_rejected)) {
        running_rejected <- bind_rows(running_rejected, rejected_list)
    } else {
        running_rejected <- rejected_list
    }

    list(next_pair_list, next_data, series_groups, running_k, rejected_list)
}

make_exclusion_table <- function(matches, exclusion, ...) {
    m <- matches |>
        filter(...)
    if (!is.null(exclusion)) {
        m <- m |> semi_join(exclusion, by = colnames(exclusion))
    }
    m <- m |> select(key_x, key_y, variable)
    bind_rows(m, m |> select(key_x = key_y, key_y = key_x, variable))
}

#' Effettua il merge di serie di dati secondo i raggruppamenti e le priorità specificate.
#' Nell'effettuare il merge vengono fatte delle correzioni alle serie che integrano modellizzando le differenze tra la serie integrante e la serie integrata con una pseudo-sinusoide di periodo un anno.
#'
#' @param data Un tibble con le colonne `date`, `key`, `variable` e `value`. Tutte le serie di dati coinvolte nel merge.
#' @param series_groups Un tibble con le colonne `gkey`, `key`, `variable` e `priority`. Indica i raggruppamenti di serie di dati e le priorità di merge. La serie con priority maggiore viene considerata come 'master', le altre vengono utilizzate in sequenza per integrare i dati mancanti.
#' @param metadata Un tibble con almeno le colonne `key`, `sensor_key` e `dataset`. Contiene i metadati delle serie di dati.
#' @param optimal_offsets Un tibble con le colonne `key_x`, `key_y`, `variable` e `offset_days`. Indica gli offset temporali ottimali tra le serie di dati coinvolte nel merge.
#' @param rejection_threshold Un valore numerico. Indica la soglia di rifiuto delle correzioni. Se la media delle correzioni annuali è maggiore di `rejection_threshold` il merge viene rifiutato.
#' @param ignore_corrections Un tibble con le colonne `key_x`, `key_y`, `variable`. Indica le coppie di serie di dati per le quali non si vogliono fare correzioni, pur mantenendo il merge.
#'
#' @return Una lista con tre elementi:
#' - `meta` Un tibble con le colonne `gkey`, `variable`, `from_keys`, `from_sensor_keys`, `from_datasets`, `key`. Indica le serie di dati coinvolte nel merge.
#' - `data` Un tibble con le colonne `date`, `variable`, `value`, `key`, `from_key`, `correction`. Indica i dati integrati.
#' - `coeffs` Un tibble con le colonne `key_x`, `key_y`, `variable`, `k0`, `k1`, `k2`, `k3`. Indica le correzioni effettuate.
dynamic_merge <- function(data, series_groups, metadata, optimal_offsets, rejection_threshold, ignore_corrections = NULL, contribution_threshold = 365L * 2L) {
    if (series_groups |> group_by(key, variable) |> count() |> filter(n > 1L) |> nrow() > 0L) {
        stop("There are series contributing to multiple groups in the series_groups table. This is not allowed.")
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
    running_rejected <- NULL
    while (nrow(pair_list) > 0L) {
        merge_results <- pairs_merge(pair_list, data, rejection_threshold, ignore_corrections, contribution_threshold)
        c(pair_list, data, series_groups, running_ks, running_rejected) %<-% update_tables(series_groups, pair_list, merge_results, data, optimal_offsets, running_ks, running_rejected)
    }
    list("meta" = key_meta, "data" = data, "coeffs" = running_ks, "rejected" = running_rejected)
}

incompatible_merges <- function(data, series_groups, optimal_offsets, cycles, rejection_threshold, ignore_corrections = NULL, contribution_threshold = 365L * 2L) {
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
    running_rejected <- NULL
    while ((n_rejected == 0L | counter < cycles) & nrow(pair_list) > 0L) {
        merge_results <- pairs_merge(pair_list, data, rejection_threshold, ignore_corrections, contribution_threshold)
        n_rejected <- nrow(merge_results$rejected)
        rejects <- rows_append(rejects, merge_results$rejected)
        c(pair_list, data, series_groups, running_ks, running_rejected) %<-% update_tables(series_groups, pair_list, merge_results, data, optimal_offsets, running_ks, running_rejected)
        counter <- counter + 1L
    }
    rejects
}
