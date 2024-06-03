library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)


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
    mutate(data_rank = row_number(), skip_correction = "ISAC" %in% network) |>
    select(set, gkey, key, data_rank, skip_correction) |>
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

load_data.group <- function(data_root, series_specs) {
  series_tags <- series_specs |>
    mutate(col_names = str_c(from_dataset, from_sensor_key, sep = "/")) |>
    pull(col_names)
  data <- series_specs |>
    rowwise() |>
    reframe(data = maybe_load(file.path(data_root, str_glue("dataset={from_dataset}"), str_glue("sensor_key={from_sensor_key}"), str_glue("variable={variable}"), "part-0.parquet")) |> mutate(dataset = from_dataset, sensor_key = from_sensor_key)) |> # read_parquet(file.path(data_root, str_glue("dataset={dataset}"), str_glue("sensor_key={sensor_key}"), str_glue("variable={variable}"), "part-0.parquet")) |> mutate(dataset = dataset, sensor_key = sensor_key)) |>
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

optimal_offset <- function(table, col1, col2) {
  if (table |> mutate(common = !is.na({{ col1 }}) & !is.na({{ col2 }})) |> filter(common) |> nrow() < 365L) {
    return(0L)
  }

  master <- select(table, date, {{ col1 }}) |>
    filter(!is.na({{ col1 }}))
  integrator <- select(table, date, {{ col2 }}) |>
    filter(!is.na({{ col2 }})) |>
    cross_join(tibble(offset = c(-1L, 0L, 1L))) |>
    mutate(date = date + days(offset))

  master |>
    inner_join(integrator, by = "date") |>
    mutate(adiff = abs({{ col1 }} - {{ col2 }})) |>
    group_by(offset) |>
    summarise(f0 = mean(as.numeric(adiff < 0.1), na.rm = TRUE), maeT = mean(adiff, na.rm = TRUE), .groups = "drop") |>
    mutate(rank_f0 = rank_f0(f0)) |>
    arrange(rank_f0, maeT) |>
    pull(offset) |>
    first()
}

merge_columns <- function(table, integrator, correction_threshold, contribution_threshold, skip_correction, force_merge) {
  offset <- optimal_offset(table, master, {{ integrator }})

  master_table <- table |>
    select(date, !{{ integrator }})
  integrator_table <- table |>
    select(date, {{ integrator }}) |>
    mutate(date = date + days(offset))
  table <- master_table |>
    full_join(integrator_table, by = "date")

  DELT <- table |>
    mutate(correction_sample = master - {{ integrator }}, month = month(date)) |>
    filter(abs(correction_sample) < 5, !is.na(correction_sample))

  if (skip_correction) {
    correction_coeffs <- list(k0 = 0, a1 = 0, a2 = 0, b1 = 0, b2 = 0)
    mean_correction <- 0
  } else {
    available_months <- DELT |>
      count(month) |>
      filter(n >= 20L) |>
      nrow()

    if (available_months >= 8L) {
      # t is computed here to avoid problems when joining with a shifted integrator
      ts <- annual_index(DELT$date)
      correction_coeffs <- sin_coeffs(DELT$correction_sample, ts) # DELT$t)
      mean_correction <- abs(correction_coeffs$k0)
    } else if (available_months > 2L) {
      correction_coeffs <- list(k0 = mean(DELT$correction_sample, na.rm = TRUE), a1 = 0, a2 = 0, b1 = 0, b2 = 0)
      mean_correction <- abs(correction_coeffs$k0)
    } else {
      correction_coeffs <- list(k0 = 0, a1 = 0, a2 = 0, b1 = 0, b2 = 0)
      mean_correction <- 0
    }
  }

  if (!force_merge &&
    ((table |> mutate(integrates = is.na(master) & !is.na({{ integrator }})) |> filter(integrates) |> nrow() < contribution_threshold) ||
      (mean_correction >= correction_threshold))
  ) {
    merged <- FALSE
  } else {
    table <- table |>
      mutate(
        t = annual_index(date),
        correction = correction_coeffs$k0 + correction_coeffs$a1 * sin(t) + correction_coeffs$a2 * sin(2 * t) + correction_coeffs$b1 * cos(t) + correction_coeffs$b2 * cos(2 * t),
      )

    # Evito di correggere le serie che si scostano di poco
    if ((mean_correction <= 0.1) && (table |>
      filter(abs(correction) > 0.2) |>
      nrow() == 0L)) {
      table <- table |> mutate(correction = 0)
      correction_coeffs <- list(k0 = 0, a1 = 0, a2 = 0, b1 = 0, b2 = 0)
      mean_correction <- 0
    }
    table <- table |>
      mutate(
        master = coalesce(master, {{ integrator }} + correction),
        from = coalesce(from, if_else(is.na({{ integrator }}), NA_character_, integrator |> ensym() |> as.character()))
      ) |>
      select(-correction)
    merged <- TRUE
  }

  list(table = table, meta = c(correction_coeffs, merged = merged, offset = offset))
}

dynamic_merge.group <- function(data_root, group_rankings, correction_threshold, contribution_threshold) {
  series_specs <- group_rankings |>
    arrange(data_rank) |>
    mutate(skip_correction = skip_correction | data_rank == min(data_rank), force_merge = data_rank == min(data_rank))

  dataset <- series_specs |>
    pull(dataset) |>
    first()
  sensor_key <- series_specs |>
    pull(sensor_key) |>
    first()
  variable <- series_specs |>
    pull(variable) |>
    first()

  c(data, series) %<-% load_data.group(data_root, series_specs)
  table <- data |>
    mutate(from = NA_character_, master = NA_real_)

  skips <- series_specs |> pull(skip_correction)
  force_merge <- series_specs |> pull(force_merge)
  params <- series_specs |> select(skip_correction, force_merge)

  merge_result <- purrr::reduce2(
    series,
    params |> purrr::transpose(),
    \(merge_result, integrator, param) {
      c(table, merge_meta) %<-% merge_result
      merge_result <- merge_columns(table, !!sym(integrator), correction_threshold, contribution_threshold, param$skip_correction, param$force_merge)
      list(table = merge_result$table, merge_meta = bind_rows(merge_meta, merge_result$meta |> as_tibble_row()))
    },
    .init = list(table = table, meta = NULL)
  )

  meta <- bind_cols(series_specs, merge_result$merge_meta)
  table <- merge_result$table |>
    select(date, from, value = master) |>
    # Due to the way the merge is done (time offsets), there may be some missing values in the master column
    filter(!is.na(value), !is.na(from)) |>
    separate_wider_delim(from, delim = "/", names = c("from_dataset", "from_sensor_key"), names_repair = "minimal", cols_remove = TRUE) |>
    mutate(from_sensor_key = as.integer(from_sensor_key))

  list(table = table, meta = meta)
}

dynamic_merge.both_var <- function(data_root, group_rankings, correction_threshold, contribution_threshold) {

}

dynamic_merge.full <- function(path_from, path_to, ranked_series_groups, correction_threshold, contribution_threshold, n_workers = future::availableCores() - 1L) {
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
  future::plan(future::multisession, workers = n_workers)

  ranked_series_groups |>
    group_split(dataset, sensor_key, variable) |>
    furrr::future_walk(
      ~ {
        dataset <- .x$dataset |> first()
        sensor_key <- .x$sensor_key |> first()
        variable <- .x$variable |> first()
        c(table, meta) %<-% dynamic_merge.group(path_from, .x, correction_threshold, contribution_threshold)
        data_path <- file.path(path_to, "data", str_glue("dataset={dataset}"), str_glue("sensor_key={sensor_key}"), str_glue("variable={variable}"))
        if (!dir.exists(data_path)) {
          dir.create(data_path, recursive = TRUE)
        }
        write_parquet(table |> arrange(date), file.path(data_path, "part-0.parquet"))
        meta_path <- file.path(path_to, "meta", str_glue("dataset={dataset}"), str_glue("sensor_key={sensor_key}"), str_glue("variable={variable}"))
        if (!dir.exists(meta_path)) {
          dir.create(meta_path, recursive = TRUE)
        }
        write_parquet(meta, file.path(meta_path, "part-0.parquet"))
      },
      .progress = FALSE
    )
}
