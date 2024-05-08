library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(progressr, warn.conflicts = FALSE)

annual_index <- function(date) {
    2 * pi * (yday(date) / yday(make_date(year(date), 12L, 31L)))
}

sin_coeffs <- function(sample, t) {
    coeffs <- lm(sample ~ sin(t / 2) + sin(t) + sin(2 * t))$coefficients |> as.list()
    names(coeffs) <- c("k0", "k1", "k2", "k3")
    coeffs
}


load_data.group <- function(data_root, series_specs) {
    series_tags <- series_specs |>
        mutate(col_names = str_c(dataset, sensor_key, sep = "/")) |>
        pull(col_names)
    data <- series_specs |>
        rowwise() |>
        reframe(data = read_parquet(file.path(data_root, str_glue("dataset={dataset}"), str_glue("sensor_key={sensor_key}"), str_glue("variable={variable}"), "part-0.parquet")) |> mutate(dataset = dataset, sensor_key = sensor_key)) |>
        unnest(everything()) |>
        pivot_wider(id_cols = date, names_from = c(dataset, sensor_key), names_sep = "/", values_from = value) |>
        relocate(all_of(series_tags), date) |>
        arrange(date)
    list(data, series_tags)
}

rank_f0 <- function(f0) {
    case_when(
        f0 >= 0.5 ~ 1L,
        f0 > 0.1 ~ 2L,
        .default = 3L
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
        inner_join(integrator, by = c("date")) |>
        group_by(offset) |>
        mutate(adiff = abs({{ col1 }} - {{ col2 }})) |>
        summarise(f0 = mean(adiff < 5e-2, na.rm = TRUE), maeT = mean(adiff, na.rm = TRUE)) |>
        mutate(rank_f0 = rank_f0(f0)) |>
        arrange(rank_f0, maeT) |>
        pull(offset) |>
        first()
}

merge_columns <- function(table, integrator, correction_threshold, contribution_threshold, skip_correction) {
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
        correction_coeffs <- list(k0 = 0, k1 = 0, k2 = 0, k3 = 0)
        mean_correction <- 0
    } else {
        available_months <- DELT |>
            count(month) |>
            filter(n >= 20L) |>
            nrow()

        if (available_months >= 8L) {
            correction_coeffs <- sin_coeffs(DELT$correction_sample, DELT$t)
            mean_correction <- abs(correction_coeffs$k0 + 2 * correction_coeffs$k1 / pi)
        } else if (available_months > 2L) {
            correction_coeffs <- list(k0 = mean(DELT$correction_sample, na.rm = TRUE), k1 = 0, k2 = 0, k3 = 0)
            mean_correction <- abs(correction_coeffs$k0)
        } else {
            correction_coeffs <- list(k0 = 0, k1 = 0, k2 = 0, k3 = 0)
            mean_correction <- 0
        }
    }

    if (
        (table |> mutate(integrates = is.na(master) & !is.na({{ integrator }})) |> filter(integrates) |> nrow() < contribution_threshold) ||
            (mean_correction > correction_threshold)
    ) {
        merged <- FALSE
    } else {
        table <- table |>
            mutate(
                master = coalesce(master, {{ integrator }} + correction_coeffs$k0 + sin(t / 2) * correction_coeffs$k1 + sin(t) * correction_coeffs$k2 + sin(2 * t) * correction_coeffs$k3),
                from = coalesce(from, replace({{ integrator }}, !is.na({{ integrator }}), integrator |> ensym() |> as.character()))
            )
        merged <- TRUE
    }

    list(table = table, meta = c(correction_coeffs, merged = merged, offset = offset))
}

dynamic_merge.group <- function(data_root, group_rankings, correction_threshold, contribution_threshold) {
    series_specs <- group_rankings |>
        arrange(rank) |>
        mutate(skip_correction = skip_correction | rank == 1L)

    set <- series_specs |>
        pull(set) |>
        first()
    gkey <- series_specs |>
        pull(gkey) |>
        first()
    variable <- series_specs |>
        pull(variable) |>
        first()

    c(data, series) %<-% load_data.group(data_root, series_specs |> mutate(variable = variable))
    table <- data |>
        mutate(t = annual_index(date), from = NA_character_, master = NA_real_)

    skips <- series_specs |> pull(skip_correction)

    merge_result <- purrr::reduce2(
        series,
        skips,
        \(merge_result, integrator, skip_correction) {
            c(table, merge_meta) %<-% merge_result
            merge_result <- merge_columns(table, !!sym(integrator), correction_threshold, contribution_threshold, skip_correction)
            list(table = merge_result$table, merge_meta = bind_rows(merge_meta, merge_result$meta |> as_tibble_row()))
        },
        .init = list(table = table, meta = NULL)
    )

    meta <- bind_cols(series_specs, merge_result$merge_meta)
    table <- merge_result$table |>
        select(date, from, value = master) |>
        separate_wider_delim(from, delim = "/", names = c("from_dataset", "from_sensor_key"), names_repair = "minimal") |>
        mutate(from_sensor_key = as.integer(from_sensor_key), set = set, gkey = gkey, variable = variable)

    list(table = table, meta = meta)
}

dynamic_merge.full <- function(path_from, path_to, ranked_series_groups, correction_threshold, contribution_threshold, n_workers = future::availableCores() - 1L) {
    future::plan(future::multisession, workers = n_workers)

    ranked_series_groups |>
        group_split(set, gkey, variable) |>
        furrr::future_walk(
            ~ {
                set <- .x$set |> first()
                gkey <- .x$gkey |> first()
                variable <- .x$variable |> first()
                c(table, meta) %<-% dynamic_merge.group(path_from, .x, correction_threshold, contribution_threshold)
                data_path <- file.path(path_to, "data", str_glue("set={set}"), str_glue("gkey={gkey}"), str_glue("variable={variable}"))
                if (!dir.exists(data_path)) {
                    dir.create(data_path, recursive = TRUE)
                }
                write_parquet(table |> arrange(date), file.path(data_path, "part-0.parquet"))
                meta_path <- file.path(path_to, "meta", str_glue("set={set}"), str_glue("gkey={gkey}"), str_glue("variable={variable}"))
                if (!dir.exists(meta_path)) {
                    dir.create(meta_path, recursive = TRUE)
                }
                write_parquet(meta, file.path(meta_path, "part-0.parquet"))
            },
            .progress = FALSE
        )
}
