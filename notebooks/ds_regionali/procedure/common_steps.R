library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(assertthat, warn.conflicts = FALSE)
library(zeallot, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/database/test.R")
source("src/database/write.R")
source("src/database/query/data.R")
source("src/database/data_model.R")
source("src/database/query/spatial.R")
source("src/analysis/data/quality_check.R")
source("src/merging/combining.R")
source("notebooks/ds_regionali/procedure/checkpoint.R")
source("notebooks/ds_regionali/procedure/plots.R")
source("notebooks/ds_regionali/procedure/tools.R")

test_metadata_consistency <- function(meta) {
    meta |> verify(!is.na(series_id))

    meta |>
        group_by(sensor_id, station_id, series_id) |>
        count() |>
        verify(n == 1L)

    assert_that(meta |>
        filter(if_all(c(sensor_id, station_id, series_id), is.na)) |>
        nrow() == 0L)

    assert_that(meta |>
        filter(if_any(c(dataset, network, lon, lat, kind), is.na)) |>
        nrow() == 0L)

    meta
}

test_data_consistency <- function(data) {
    if (data |>
        filter(if_any(c(dataset, sensor_key, variable, date), is.na)) |>
        compute() |>
        nrow() > 0L) {
        stop("Data contains NA keys")
    }

    if (data |> group_by(dataset, sensor_key, variable, date) |> count() |> filter(n > 1L) |> compute() |> nrow() > 0L) {
        stop("Data contains duplicate measures for the same key")
    }

    data
}

make_keys <- function(meta) {
    meta |>
        collect() |>
        mutate(
            sensor_key = row_number(),
            dummy_station_id = coalesce(station_id, series_id)
        ) |>
        group_by(dummy_station_id) |>
        mutate(station_key = cur_group_id()) |>
        ungroup() |>
        select(-dummy_station_id) |>
        group_by(series_id) |>
        mutate(series_key = cur_group_id()) |>
        ungroup()
}

#' Given a measures table and a metadata table, associates the id given in the metadata table to the measures table's key
#' and returns the resulting table.
associate_sensor_key <- function(data, meta) {
    common_id <- intersect(names(data |> select(ends_with("_id"), any_of("user_code"))), names(meta |> select(ends_with("_id"), user_code)))
    data |>
        left_join(
            meta |> mutate(sensor_first = coalesce(sensor_first, station_first, series_first), sensor_last = coalesce(sensor_last, station_last, series_last)) |> select(all_of(common_id), sensor_key, sensor_first, sensor_last),
            by = common_id,
            relationship = "many-to-many"
        ) |>
        filter((is.na(sensor_first) | sensor_first <= date) & (date <= sensor_last | is.na(sensor_last))) |>
        select(!c(all_of(common_id), sensor_first, sensor_last))
}

date_meta <- function(data, meta) {
    data |>
        left_join(meta |> select(dataset, sensor_key, station_key, series_key), by = c("dataset", "sensor_key"), relationship = "many-to-one") |>
        group_by(dataset, sensor_key, station_key, series_key) |>
        summarise(
            sensor_first = min(date, na.rm = FALSE),
            sensor_last = max(date, na.rm = FALSE),
        ) |>
        collect() |>
        group_by(dataset, station_key) |>
        mutate(
            station_first = min(sensor_first, na.rm = FALSE),
            station_last = max(sensor_last, na.rm = FALSE),
        ) |>
        group_by(dataset, series_key) |>
        mutate(
            series_first = min(sensor_first, na.rm = FALSE),
            series_last = max(sensor_last, na.rm = FALSE),
        ) |>
        ungroup()
}

associate_dem_elevation <- function(metadata, statconn) {
    metadata |> query_elevations(statconn)
}

associate_dem_elevation.bruno <- function(metadata, glo30_dem_path) {
    dst <- stars::st_mosaic(
        list.files(
            glo30_dem_path,
            recursive = TRUE,
            full.names = TRUE,
            pattern = "*_DEM.tif$"
        )
    )
    dem <- stars::read_stars(dst, proxy = TRUE)
    metadata |>
        mutate(elevation_glo30 = stars::st_extract(dem, pick(lon, lat) |> as.matrix()) |> pull(1L))
}

associate_regional_info.bruno <- function(metadata, province_boundaries_path) {
    province_boundaries <- sf::st_read(province_boundaries_path, quiet = TRUE)
    metadata |>
        st_md_to_sf() |>
        st_join(province_boundaries |> select(province_full = shapeName), join = sf::st_intersects) |>
        sf::st_drop_geometry()
}


prepare_daily_data <- function(data_pack, statconn) {
    data_pack$meta <- data_pack$meta |>
        arrange(name, series_id, user_code) |>
        make_keys() |>
        associate_regional_info(statconn) |>
        associate_dem_elevation(statconn) |>
        mutate(sensor_first = coalesce(sensor_first, station_first), sensor_last = coalesce(sensor_last, station_last)) |>
        test_metadata_consistency() |>
        as_arrow_table()

    data_pack$data <- data_pack$data |>
        filter(!is.na(value)) |>
        mutate(variable = if_else(variable == "T_MIN", -1L, 1L)) |>
        associate_sensor_key(data_pack$meta) |>
        test_data_consistency() |>
        arrange(sensor_key, variable, date) |>
        compute()

    date_metas <- date_meta(data_pack$data, data_pack$meta) |>
        as_arrow_table()

    meta <- data_pack$meta |>
        select(!c(ends_with("_first"), ends_with("_last"))) |>
        left_join(date_metas, by = c("dataset", "sensor_key", "station_key", "series_key")) |>
        select(all_of(names(meta_schema))) |>
        compute()
    extra_meta <- data_pack$meta |>
        select(-names(meta_schema), sensor_key, dataset) |>
        compute()

    # split <- split_station_metadata(data_pack$meta)
    list("checkpoint" = as_checkpoint(meta, data_pack$data), "extra_meta" = extra_meta)
}

qc_checkpoint <- function(dataset, conn) {
    ds <- query_checkpoint(dataset, "raw", conn)
    qc_data <- qc1(ds$data)
    qc_meta <- ds$meta
    as_checkpoint(meta = qc_meta |> to_arrow(), data = qc_data |> to_arrow(), check_schema = FALSE) |> save_checkpoint(dataset, "qc1", check_schema = FALSE, partitioning = c("valid", "variable"))
}

#' Produces the plot of year-monthly series availabilities (the number of available and usable series per year/month) and the table used to compute them.
#' The plot is faceted by variable.
#'
#' @param data The data to be used in standard data format.
#' @param ... Additional arguments to be passed to monthly_availabilities (the availability thresholds).
#'
#' @return A list containing the plot and the data.
ymonthly_availabilities <- function(data, stack = FALSE, ...) {
    plot_district_avail.tbl(
        data, stack, ...
    )
}

#' Produces the table and plot of spatial series availabilities.
spatial_availabilities <- function(ymonthly_avail, stations, map, ...) {
    spatav <- clim_availability(ymonthly_avail, ...) |> collect()
    # collect()

    p <- ggplot() +
        geom_sf(data = map) +
        geom_sf(
            data = spatav |>
                inner_join(stations |> select(dataset, sensor_key, lon, lat) |> collect(), join_by(dataset, sensor_key)) |>
                st_md_to_sf(),
            aes(color = qc_clim_available, shape = dataset)
        )
    list("plot" = p, "data" = spatav)
}

prepare_data_for_merge.old <- function(dataconn, ds_root, regenerate = FALSE) {
    if (regenerate || !fs::dir_exists(ds_root)) {
        # save_ds <- function(ds) {
        #     fragment_dir <- fs::path(
        #         ds_root,
        #         str_glue("dataset={first(ds$dataset)}"),
        #         str_glue("sensor_key={first(ds$sensor_key)}"),
        #         str_glue("variable={first(ds$variable)}")
        #     )
        #     if (!fs::dir_exists(fragment_dir)) {
        #         fs::dir_create(fragment_dir, recurse = TRUE)
        #     }
        #     write_parquet(ds |> arrange(date), fs::path(fragment_dir, "part-0.parquet"))
        # }

        # datasets <- fs::dir_ls(fs::path_dir(archive_path("*", "data", "raw")), type = "directory") |> fs::path_file()

        # query_checkpoint_data(datasets, "raw", dataconn, hive_types = list("variable" = "INT")) |>
        #     filter(variable == -1L) |>
        #     collect() |>
        #     group_split(dataset, sensor_key, variable) |>
        #     purrr::walk(save_ds, .progress = TRUE)

        # query_checkpoint_data(datasets, "raw", dataconn, hive_types = list("valid" = "BOOLEAN", "variable" = "INT")) |>
        #     filter(valid, variable == 1L) |>
        #     select(!c(starts_with("qc_"), valid)) |>
        #     collect() |>
        #     group_split(dataset, sensor_key, variable) |>
        #     purrr::walk(save_ds, .progress = TRUE)
        # to_arrow() |>
        # write_dataset(ds_root, format = "parquet", partitioning = c("dataset", "sensor_key", "variable")) BUGGED: too many files
        DBI::dbExecute(dataconn, "COPY")
    }
    ds_root
}

prepare_data_for_merge <- function(dataconn, from_raw_root, to_ds_root, regenerate = TRUE, test = FALSE) {
    if (regenerate || !fs::dir_exists(to_ds_root)) {
        ds_files <- fs::path(from_raw_root, "**", "*.parquet")
        DBI::dbExecute(conns$data, stringr::str_glue("CREATE OR REPLACE VIEW ds_4merge_tmp AS SELECT * FROM read_parquet('{ds_files}', hive_partitioning = true, hive_types = {{'variable': INT}})"))
        DBI::dbExecute(conns$data, stringr::str_glue("COPY ds_4merge_tmp TO '{to_ds_root}' (FORMAT PARQUET, PARTITION_BY (dataset, sensor_key, variable), FILENAME_PATTERN 'part-{{i}}')"))
    }

    if (test) {
        if (query_dataset(from_raw_root, dataconn, hive_types = list("variable" = "INT")) |>
            anti_join(
                query_dataset(to_ds_root, dataconn, hive_types = list("dataset" = "VARCHAR", "sensor_key" = "INT", "variable" = "INT")),
                by = c("dataset", "sensor_key", "variable", "date")
            ) |>
            count() |>
            collect() |>
            pull(n) > 0L) {
            stop("Data was not correctly copied")
        }
    }

    to_ds_root
}

merge_same_series <- function(path_from, path_to, set_name, tagged_analysis, metadata, data, correction_threshold, contribution_threshold, dataset_rankings, ..., .f0_epsilon = 0.05) {
    gs <- series_groups(tagged_analysis, metadata, data, tag_same_series)
    ranked_series_groups <- gs$table |>
        group_by(gkey, key) |>
        filter(n() == 2L) |> # Tiene solo i match in cui ci sono sia le minime che le massime
        ungroup() |>
        mutate(set = set_name) |>
        rank_metadata(metadata, dataset_rankings, ...) |>
        rank_data(metadata) |>
        left_join(metadata |> select(from_dataset = dataset, from_sensor_key = sensor_key, key, network), by = "key") |>
        rename(dataset = set, series_key = gkey) |>
        group_by(series_key, variable) |>
        mutate(only_recent = ("ISAC" %in% network) & (network != "ISAC")) |> # Se c'è una serie ISAC (omogeneizzata) aggiorna solo i dati recenti
        ungroup() |>
        select(-network)

    dynamic_merge(path_from, path_to, ranked_series_groups, correction_threshold, contribution_threshold, .f0_epsilon)
    path_to
}

merged_checkpoint <- function(set_name, merge_results_path, metadata) {
    # Saving the merging specs: correction coefficients, merged status, series groups
    merging_specs <- open_dataset(fs::path(merge_results_path, "meta", str_glue("dataset={set_name}")), format = "parquet") |>
        select(-key) |>
        collect() |>
        relocate(dataset, series_key, .before = 1L)

    specs_path <- fs::path(archive_path(set_name, "extra", "merge_specs"), ext = "parquet")
    if (!fs::dir_exists(fs::path_dir(specs_path))) {
        fs::dir_create(fs::path_dir(specs_path), recurse = TRUE)
    }
    merging_specs |>
        write_parquet(specs_path)

    data <- open_dataset(
        fs::path(merge_results_path, "data", str_glue("dataset={set_name}")),
        format = "parquet",
        partitioning = hive_partition(series_key = int32(), variable = int32())
    ) |> mutate(dataset = set_name)

    datestats90 <- data |>
        filter(year(date) >= 1990L, !is.na(value)) |>
        count(dataset, series_key, variable) |>
        group_by(dataset, series_key) |>
        summarise(valid90 = max(n), .groups = "drop") |>
        compute()

    datestats <- data |>
        filter(!is.na(value)) |>
        group_by(dataset, series_key, variable) |>
        summarise(valid_days = n(), series_first = min(date), series_last = max(date), .groups = "drop_last") |>
        summarise(series_first = min(series_first), series_last = max(series_last), valid_days = max(valid_days)) |>
        full_join(datestats90, by = c("dataset", "series_key")) |>
        collect()

    metadata <- metadata |>
        rename(from_dataset = dataset, from_sensor_key = sensor_key) |>
        select(-any_of(c("key", "series_key"))) |>
        inner_join(merging_specs |> select(from_dataset, from_sensor_key, dataset, series_key, metadata_rank, data_rank, merged) |> distinct(), by = c("from_dataset", "from_sensor_key"), relationship = "one-to-one") |>
        # filter(merged) |>
        group_by(dataset, series_key) |>
        arrange(metadata_rank, .by_group = TRUE) |>
        summarise(
            across(!c(ends_with("_first"), ends_with("_last"), from_dataset, from_sensor_key, data_rank, merged), ~ first(.)),
            from_sensor_keys = list(from_sensor_key),
            from_datasets = list(from_dataset),
            data_ranks = list(data_rank),
            merged = list(merged),
            .groups = "drop"
        ) |>
        select(
            dataset, series_key, name, user_code, network, country, district, province_code, town, lon, lat, elevation, elevation_glo30, kind,
            from_datasets, from_sensor_keys, data_ranks, merged
        ) |>
        left_join(datestats, by = c("dataset", "series_key"), relationship = "one-to-one") |>
        # filter(valid_days >= 30L) |>
        as_arrow_table(
            schema = schema(
                dataset = utf8(),
                series_key = int32(),
                name = utf8(),
                user_code = utf8(),
                network = utf8(),
                country = utf8(),
                district = utf8(),
                province_code = utf8(),
                town = utf8(),
                lon = float64(),
                lat = float64(),
                elevation = float64(),
                elevation_glo30 = float64(),
                kind = utf8(),
                from_datasets = list_of(utf8()),
                from_sensor_keys = list_of(int32()),
                data_ranks = list_of(int32()),
                merged = list_of(bool()),
                series_first = date32(),
                series_last = date32(),
                valid_days = int32(),
                valid90 = int32(),
            )
        )

    # Filtering out short series
    # data <- data |>
    #     semi_join(metadata |> select(dataset, series_key), by = c("dataset", "series_key"))

    # return(list(compute(metadata), data))
    list(meta = compute(metadata), data = compute(data)) |> save_checkpoint(set_name, "merged", check_schema = FALSE, key = "series_key")
}

test_tagged_analysis <- function(state_dir) {
    tao <- openxlsx::read.xlsx(fs::path("notebooks", "ds_regionali", state_dir, "tagged_analysis.old.xlsx"))
    ta <- openxlsx::read.xlsx(fs::path("notebooks", "ds_regionali", state_dir, "tagged_analysis.xlsx"))

    problems <- tao |>
        full_join(ta, by = c("dataset_x", "sensor_key_x", "variable", "dataset_y", "sensor_key_y"), suffix = c(".old", ".new")) |>
        filter(is.na(tag_same_series.old) | is.na(tag_same_series.new) | tag_same_series.old != tag_same_series.new)


    if (problems |> nrow() > 0L) {
        warning("Tagged analysis is not consistent")
    }

    problems
}
