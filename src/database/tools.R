library(arrow, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(rlang, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/database/data_model.R")
source("src/load/ITA.R")

as_arrow_table2.data.frame <- function(table, schema) {
    table |>
        as_tibble() |>
        select(all_of(schema$names)) |>
        as_arrow_table(schema = schema)
}

as_arrow_table2.ArrowObject <- function(table, schema) {
    table <- table |>
        select(all_of(schema$names)) |>
        compute()
    table$cast(schema)
}

as_arrow_table2.arrow_dplyr_query <- function(table, schema) {
    table <- table |>
        select(all_of(schema$names)) |>
        compute()
    table$cast(schema)
}

as_arrow_table2 <- function(x, ...) UseMethod("as_arrow_table2", x)

split_station_metadata <- function(full_station_list) {
    base_meta <- select(full_station_list, all_of(meta_schema$names)) |> as_arrow_table2(meta_schema)
    extra_meta <- select(full_station_list, !all_of(meta_schema$names), dataset, sensor_key) |> as_arrow_table()
    list("base" = base_meta, "extra" = extra_meta)
}

archive_path <- function(dataset, what, step) {
    file.path("db", what, step, dataset)
}

archive_files <- function(dataset, what, step) {
    fs::dir_ls(archive_path(dataset, what, step), recurse = TRUE, glob = "*.parquet")
}

fill_regional_na <- function(metadata, statconn) {
    copy_to(statconn, metadata, name = "_m_tmp", overwrite = TRUE)
    metadata <- dbGetQuery(
        statconn,
        "
        SELECT m.*, b.name AS fill_province
        FROM _m_tmp m
        LEFT JOIN boundary b
        ON ST_Contains(b.geom, ST_SetSRID(ST_MakePoint(lon, lat), 4326)) AND b.kind = 'province'
        "
    )
    dbRemoveTable(statconn, "_m_tmp")
    metadata |>
        mutate(province = coalesce(province, fill_province)) |>
        select(-fill_province)
}

add_missing_columns <- function(metadata, cols) {
    for (col in cols) {
        if (!(col %in% colnames(metadata))) {
            metadata <- metadata |> mutate("{col}" := NA_character_)
        }
    }
    metadata
}

prepare_for_regional_completion <- function(metadata) {
    metadata <- add_missing_columns(metadata, c("country", "province_full", "province_code", "district"))
    if ("province" %in% colnames(metadata)) {
        metadata <- metadata |> mutate(
            province_full = coalesce(province_full, if_else(!is.na(province) & str_length(province) == 2L, NA_character_, province)) |> str_to_title(),
            province_code = coalesce(province_code, if_else(!is.na(province) & str_length(province) == 2L, province, NA_character_)) |> str_to_upper(),
            .keep = "unused"
        )
    }
    metadata
}

associate_regional_info <- function(metadata, geoconn, info_path = fs::path(file.path("external", "province_regioni.csv"))) {
    metadata |>
        count(dataset, sensor_key) |>
        assertr::verify(n == 1L, description = "Multiple metadata rows for the same sensor")

    # NA è Napoli
    regional_info <- vroom::vroom(info_path, col_types = "ccc", na = c("")) |>
        rename(district = state) |>
        mutate(
            province_k = province_full |> str_to_lower() |> str_squish() |> str_remove_all(regex("[^a-z]")),
            province_code = str_to_upper(province_code),
            province_full = str_to_title(province_full)
        )
    geo <- st_read(geoconn, "regional_boundaries", geometry_column = "geometry", quiet = TRUE) |> mutate(shapeName = str_to_title(shapeName))

    districts <- geo |>
        filter(kind == "district") |>
        select(district = shapeName, country)
    provinces <- geo |>
        filter(kind == "municipality") |>
        select(province_full = shapeName)

    metadata <- metadata |>
        prepare_for_regional_completion() |>
        st_md_to_sf() |>
        st_join(districts, join = st_intersects, suffix = c(".provided", ".computed"), left = TRUE) |>
        st_join(provinces, join = st_intersects, suffix = c(".provided", ".computed"), left = TRUE) |>
        st_drop_geometry() |>
        mutate(
            country = coalesce(country.provided, country.computed) |> str_to_title(),
            district = coalesce(district.provided, district.computed) |> str_to_title(),
            province_full = coalesce(province_full.provided, province_full.computed) |> str_to_title(),
            .keep = "unused"
        ) |>
        mutate(province_k = province_full |> str_to_lower() |> str_squish() |> str_remove_all(regex("[^a-z]")), .keep = "unused") |>
        left_join(regional_info, by = "province_k", suffix = c(".provided", ".table"), relationship = "many-to-one") |>
        mutate(province_code = coalesce(province_code.provided, province_code.table) |> str_to_upper(), district = coalesce(district.provided, district.table), .keep = "unused") |>
        left_join(regional_info, by = "province_code", suffix = c(".provided", ".table"), relationship = "many-to-one") |>
        mutate(province_full = coalesce(province_full.provided, province_full.table), district = coalesce(district.provided, district.table), .keep = "unused") |>
        select(-starts_with("province_k")) |>
        mutate(across(c("country", "district", "province_full"), str_to_title), province_code = str_to_upper(province_code)) |>
        group_by(dataset, sensor_key) |>
        arrange(country, district, province_full, .by_group = TRUE) |>
        slice_tail() |> # In caso di sovrapposizioni dei confini regionali, prendo l'ultimo match. In particolare i confini forniti per la Francia sono pessimi
        ungroup()

    metadata |>
        count(dataset, sensor_key) |>
        assertr::verify(n == 1L, description = "Multiple metadata rows for the same sensor")

    metadata
}
