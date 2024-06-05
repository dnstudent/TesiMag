library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)


source("src/database/write.R")
source("src/merging/pairing.R")
source("src/merging/tools.R")

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

set_dataset_rank <- function(metadata, dataset_priority) {
  metadata |> mutate(dataset = factor(dataset, levels = rev(dataset_priority), ordered = TRUE))
}

rank_metadata <- function(series_groups, metadata, dataset_rankings, ...) {
  # Metadata ranking is tunable
  gkey_rank <- series_groups |>
    left_join(metadata |> select(!any_of(colnames(series_groups)), key), by = "key") |>
    set_dataset_rank(dataset_rankings) |>
    arrange(...) |>
    distinct(set, gkey, key) |>
    group_by(set, gkey) |>
    mutate(metadata_rank = row_number()) |>
    ungroup() |>
    select(set, gkey, key, metadata_rank)
  series_groups |>
    left_join(gkey_rank, by = c("set", "gkey", "key"))
}

rank_data <- function(series_groups, metadata) {
  # Data ranking is fixed: ISAC > ARPA > SCIA > DPC
  network_rank_table <- tribble(
    ~dataset, ~network, ~network_rank,
    "ISAC", "ISAC", 1L, # ISAC series are always ranked first
    "ISAC", "DPC", 4L # DPC series are always ranked last
  ) |>
    bind_rows(
      metadata |> filter(dataset == "SCIA") |> distinct(dataset, network) |> mutate(network_rank = 3L), # SCIA series are ranked second to last
      metadata |>
        filter(!dataset %in% c("SCIA", "ISAC")) |>
        distinct(dataset, network) |>
        mutate(network_rank = 2L) # ARPA series are ranked second
    )

  gkey_rank <- series_groups |>
    distinct(set, gkey, key) |>
    left_join(metadata |> select(key, dataset, sensor_key, network, sensor_last), by = "key") |>
    left_join(network_rank_table, by = c("dataset", "network")) |>
    group_by(set, gkey) |>
    arrange(network_rank, desc(sensor_last), .by_group = TRUE) |>
    mutate(data_rank = row_number(), force_zero_correction = "ISAC" %in% network) |>
    select(set, gkey, key, data_rank, force_zero_correction) |>
    ungroup()
  series_groups |>
    left_join(gkey_rank, by = c("set", "gkey", "key"))
}

sin_coeffs <- function(sample, t) {
  coeffs <- lm(sample ~ sin(t) + sin(2 * t) + cos(t) + cos(2 * t))$coefficients |> as.list()
  names(coeffs) <- c("k0", "a1", "a2", "b1", "b2")
  coeffs
}

maybe_load <- function(path, as_data_frame = TRUE) {
  if (file.exists(path)) {
    read_parquet(path, as_data_frame = as_data_frame)
  } else {
    if (as_data_frame) {
      tibble(date = NA_Date_, value = NA_real_)
    } else {
      arrow_table(schema = schema(date = date32(), value = float64()))
    }
  }
}

load_series_data <- function(data_root, series_specs) {
  series_tags <- series_specs |>
    mutate(col_names = str_c(from_dataset, from_sensor_key, sep = "/")) |>
    pull(col_names)

  series_specs |>
    rowwise() |>
    reframe(data = maybe_load(fs::path(data_root, str_glue("dataset={from_dataset}"), str_glue("sensor_key={from_sensor_key}"), str_glue("variable={variable}"), "part-0.parquet"))) |>
    unnest(everything()) |>
    pivot_wider(id_cols = c(date, variable), names_from = c(dataset, sensor_key), names_sep = "/", values_from = value) |>
    relocate(all_of(series_tags), date, variable)
}

load_data.group <- function(data_root, series_specs) {
  series_tags <- series_specs |>
    mutate(col_names = str_c(from_dataset, from_sensor_key, sep = "/")) |>
    pull(col_names)
  data <- series_specs |>
    rowwise() |>
    reframe(data = maybe_load(file.path(data_root, str_glue("dataset={from_dataset}"), str_glue("sensor_key={from_sensor_key}"), str_glue("variable={variable}"), "part-0.parquet")) |> mutate(dataset = from_dataset, sensor_key = from_sensor_key)) |>
    unnest(everything()) |>
    pivot_wider(id_cols = date, names_from = c(dataset, sensor_key), names_sep = "/", values_from = value) |>
    relocate(all_of(series_tags), date)
  list(data, series_tags)
}

load_data.group.1 <- function(data_root, series_specs) {
  series_tags <- series_specs |>
    arrange(data_rank) |>
    mutate(col_names = str_c(from_dataset, from_sensor_key, sep = "/")) |>
    pull(col_names)
  data <- series_specs |>
    rowwise() |>
    reframe(data = maybe_load(fs::path(data_root, str_glue("dataset={from_dataset}"), str_glue("sensor_key={from_sensor_key}"), str_glue("variable={variable}"), "part-0.parquet")) |>
      mutate(dataset = from_dataset, sensor_key = from_sensor_key, date = date + days(offset))) |>
    unnest(everything()) |>
    pivot_wider(id_cols = date, names_from = c(dataset, sensor_key), names_sep = "/", values_from = value) |>
    relocate(all_of(series_tags), date)
  list(data, series_tags)
}

rank_f0 <- function(f0) {
  case_when(
    0.5 < f0 ~ 1L,
    0.1 < f0 & f0 <= 0.5 ~ 2L,
    f0 <= 0.1 ~ 3L,
    .default = NA_integer_
  )
}

optimal_offset <- function(table, col1, col2, epsilon) {
  if (n_common_nnas(pull(table, {{ col1 }}), pull(table, {{ col2 }})) < 365L) {
    return(0L)
  }

  fixed <- table |>
    select(date, variable, value = {{ col1 }}) |>
    filter(!is.na(value))

  to_shift <- table |>
    select(date, variable, value = {{ col2 }}) |>
    filter(!is.na(value))

  to_shift <- to_shift |>
    cross_join(tibble(offset = c(-1L, 0L, 1L))) |>
    mutate(date = date + days(offset))

  fixed |>
    inner_join(to_shift, by = c("date", "variable"), suffix = c("_fixed", "_shifting"), relationship = "one-to-many") |>
    mutate(adiff = abs(value_fixed - value_shifting)) |>
    group_by(offset) |>
    summarise(f0 = mean(as.numeric(adiff < epsilon), na.rm = TRUE), maeT = mean(adiff, na.rm = TRUE), .groups = "drop") |>
    mutate(rank_f0 = rank_f0(f0)) |>
    arrange(rank_f0, maeT) |>
    pull(offset) |>
    first()
}

#' Offset a column by a given number of days.
offset_column_by <- function(table, column, offset, ids = c("variable")) {
  to_shift <- table |>
    select(date, all_of(ids), {{ column }}) |>
    mutate(date = date + days(offset))
  table |>
    select(-{{ column }}) |>
    full_join(to_shift, by = c("date", ids), relationship = "one-to-one")
}

insert_integrations <- function(table, integrator_column, correction_threshold, contribution_threshold, force_zero_correction, force_merge, epsilon) {
  offset <- optimal_offset(table, master, {{ integrator_column }}, epsilon)

  table <- table |>
    offset_column_by({{ integrator_column }}, offset, ids = "variable")

  merge_meta <- list(
    merged = NULL,
    few_data = FALSE,
    no_data = FALSE,
    minor_correction = FALSE,
    offset = offset,
    coeffs = NULL,
    would_integrate = (is.na(table$master) & !is.na(pull(table, {{ integrator_column }}))) |>
      as.integer() |>
      sum(),
    n_joint_days = n_common_nnas(table$master, pull(table, {{ integrator_column }})),
    mean_correction = NULL,
    failed_integrations_threshold = NULL,
    failed_correction_threshold = NULL
  )
  merge_meta$failed_integrations_threshold <- merge_meta$would_integrate < contribution_threshold

  DELT <- table |>
    mutate(correction_sample = master - !!sym(integrator_column), month = month(date)) |>
    filter(abs(correction_sample) < 5, !is.na(correction_sample))

  if (!force_zero_correction) {
    available_months <- DELT |>
      count(month) |>
      filter(n >= 20L) |>
      nrow()

    if (available_months >= 8L) {
      # t is computed here to avoid problems when joining with a shifted integrator
      ts <- annual_index(DELT$date)
      merge_meta$coeffs <- sin_coeffs(DELT$correction_sample, ts) # DELT$t)
      merge_meta$mean_correction <- merge_meta$coeffs$k0
    } else if (available_months > 2L) {
      merge_meta$coeffs <- list(k0 = mean(DELT$correction_sample, na.rm = TRUE), a1 = 0, a2 = 0, b1 = 0, b2 = 0)
      merge_meta$mean_correction <- merge_meta$coeffs$k0
      merge_meta$few_data <- TRUE
    } else {
      merge_meta$coeffs <- list(k0 = 0, a1 = 0, a2 = 0, b1 = 0, b2 = 0)
      merge_meta$mean_correction <- 0
      merge_meta$no_data <- TRUE
    }
  } else {
    merge_meta$coeffs <- list(k0 = 0, a1 = 0, a2 = 0, b1 = 0, b2 = 0)
    merge_meta$mean_correction <- 0
  }

  merge_meta$failed_correction_threshold <- abs(merge_meta$mean_correction) >= correction_threshold
  merge_meta$merged <- force_merge || (!merge_meta$failed_integrations_threshold && !merge_meta$failed_correction_threshold)

  if (merge_meta$merged) {
    table <- table |>
      mutate(
        t = annual_index(date),
        correction = merge_meta$coeffs$k0 + merge_meta$coeffs$a1 * sin(t) + merge_meta$coeffs$a2 * sin(2 * t) + merge_meta$coeffs$b1 * cos(t) + merge_meta$coeffs$b2 * cos(2 * t),
      )

    # Evito di correggere le serie che si scostano di poco
    if ((abs(merge_meta$mean_correction) <= 0.1) && all(abs(table$correction) <= 0.2, na.rm = TRUE)) {
      table <- table |> mutate(correction = 0)
      merge_meta$coeffs <- list(k0 = 0, a1 = 0, a2 = 0, b1 = 0, b2 = 0)
      merge_meta$mean_correction <- 0
      merge_meta$minor_correction <- TRUE
    }
    table <- table |>
      mutate(
        master = coalesce(master, !!sym(integrator_column) + correction),
        from = coalesce(from, if_else(is.na(master), NA_character_, integrator_column))
      ) |>
      select(-correction)
  }

  merge_meta$coeffs <- list(merge_meta$coeffs)
  list(table = table, meta = as_tibble_row(merge_meta))
}

merge_sensor <- function(table, integrator_column, correction_threshold, contribution_threshold, force_zero_correction, force_merge, epsilon) {
  tmin_results <- table |>
    filter(variable == -1L) |>
    insert_integrations({{ integrator_column }}, correction_threshold, contribution_threshold, force_zero_correction, force_merge, epsilon)

  tmax_results <- table |>
    filter(variable == 1L) |>
    insert_integrations({{ integrator_column }}, correction_threshold, contribution_threshold, force_zero_correction, force_merge, epsilon)

  meta <- bind_rows(tmin_results$meta |> mutate(variable = -1L), tmax_results$meta |> mutate(variable = 1L)) |>
    rename(would_merge = merged) |>
    mutate(only_some_var_failed = !all(would_merge) & any(would_merge), merged = all(would_merge))

  if (all(meta$merged)) {
    table <- bind_rows(tmin_results$table, tmax_results$table)
  }

  list(data = table, meta = meta)
}

dynamic_merge.series <- function(data_root, dataset, series_key, specs, correction_threshold, contribution_threshold, epsilon) {
  specs <- specs |>
    group_by(variable) |>
    mutate(force_zero_correction = force_zero_correction | (data_rank == min(data_rank)), force_merge = data_rank == min(data_rank)) |>
    ungroup()

  specs_list <- specs |>
    tidyr::nest(variables = variable) |>
    mutate(tag = str_c(from_dataset, from_sensor_key, sep = "/")) |>
    arrange(data_rank) |>
    rowwise() |>
    group_split()

  data <- load_series_data(data_root, specs)

  series_merge_results <- purrr::reduce(
    specs_list,
    \(results, spec) {
      sensor_merge_results <- merge_sensor(results$data, spec$tag, correction_threshold, contribution_threshold, spec$force_zero_correction, spec$force_merge, epsilon)
      sensor_merge_results$meta <- sensor_merge_results$meta |> mutate(dataset = dataset, series_key = series_key, from_dataset = spec$from_dataset, from_sensor_key = spec$from_sensor_key)
      list(
        data = sensor_merge_results$data,
        meta = bind_rows(results$meta, sensor_merge_results$meta)
      )
    },
    .init = list(
      data = data |>
        mutate(master = NA_real_, from = NA_character_),
      meta = tibble()
    )
  )

  merge_result <- list(
    data = series_merge_results$data |>
      select(variable, date, value = master, from) |>
      # Due to the way the merge is done (time offsets), there may be some missing values in the master column
      filter(!is.na(value)) |>
      separate_wider_delim(from, delim = "/", names = c("from_dataset", "from_sensor_key"), names_repair = "minimal", cols_remove = TRUE) |>
      mutate(from_sensor_key = as.integer(from_sensor_key)),
    meta = full_join(specs, series_merge_results$meta, by = c("from_dataset", "from_sensor_key", "variable"), relationship = "one-to-one") |> unnest_wider(coeffs)
  )

  # consistency tests
  merge_result$data |>
    assert(not_na, date, variable, value, from_dataset, from_sensor_key, description = "Absence of missing entries in the merged table")

  if (nrow(merge_result$data) < length(pull(data, specs_list[[1]]$tag[[1]]) |> na.omit())) {
    stop("There are fewer data than at the beginning")
  }

  merge_result$meta |>
    assert(not_na, everything(), description = "Absence of missing metadata in the merged table")

  merge_result
}


dynamic_merge <- function(path_from, path_to, ranked_series_groups, correction_threshold, contribution_threshold, f0_epsilon, n_workers = future::availableCores() - 1L) {
  sets <- ranked_series_groups |>
    pull(dataset) |>
    unique()
  for (set in sets) {
    dp <- fs::path(path_to, "data", str_glue("dataset={set}"))
    if (dir.exists(dp)) {
      unlink(dp, recursive = TRUE)
      unlink(fs::path(path_to, "meta", str_glue("dataset={set}")), recursive = TRUE)
    }
  }

  # Controllo che le serie abbiano tutte sia le minime che le massime
  ranked_series_groups |>
    count(dataset, series_key, from_dataset, from_sensor_key) |>
    assert(in_set(2L, allow.na = FALSE), n, description = "Presence of both minimum and maximum series for each pair of series")

  future::plan(future::multisession, workers = n_workers)

  ranked_series_groups |>
    tidyr::nest(.by = c(dataset, series_key)) |>
    rowwise() |>
    group_split() |>
    furrr::future_walk(
      ~ {
        dataset <- .x$dataset
        series_key <- .x$series_key

        c(table, meta) %<-% dynamic_merge.series(path_from, dataset, series_key, .x$data[[1]], correction_threshold, contribution_threshold, f0_epsilon)
        data_path <- file.path(path_to, "data", str_glue("dataset={dataset}"), str_glue("series_key={series_key}"))
        dir.create(data_path, recursive = TRUE)
        write_dataset(table |> arrange(variable, date), data_path, partitioning = "variable")
        # write_parquet(table |> arrange(date), file.path(data_path, "part-0.parquet"))
        meta_path <- file.path(path_to, "meta", str_glue("dataset={dataset}"), str_glue("series_key={series_key}"))
        dir.create(meta_path, recursive = TRUE)
        write_dataset(meta, meta_path, partitioning = "variable")
      },
      .progress = FALSE
    )
}
