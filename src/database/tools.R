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
    file.path("db", what, step, paste0(dataset, ".parquet"))
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

associate_regional_info <- function(metadata, statconn = NULL) {
    regional_info <- read.csv(file.path("external", "province_regioni.csv"), na.strings = c(""))
    if ("province" %in% colnames(metadata)) {
        metadata |>
            fill_regional_na(statconn) |>
            select(!any_of(c("province_code", "province_full"))) |>
            mutate(
                province_full = if_else(!is.na(province) & str_length(province) == 2L, NA_character_, province),
                province_code = if_else(!is.na(province) & str_length(province) == 2L, province, NA_character_)
            ) |>
            left_join(regional_info, by = "province_code", copy = TRUE) |>
            left_join(regional_info, by = c("province_full.x" = "province_full"), copy = TRUE) |>
            mutate(
                province_full = coalesce(province_full.x, province_full.y),
                province_code = coalesce(province_code.x, province_code.y),
                state = coalesce(state.x, state.y, state)
            ) |>
            select(!c(province, province_full.x, province_full.y, province_code.x, province_code.y, state.x, state.y))
    } else if ("province_code" %in% colnames(metadata)) {
        metadata |>
            select(!any_of("province_full")) |>
            left_join(regional_info, by = "province_code", copy = TRUE)
    } else if ("province_full" %in% colnames(metadata)) {
        metadata |>
            mutate(province_join = province_full |> str_squish() |> str_to_lower()) |>
            select(-province_full) |>
            left_join(
                regional_info |>
                    mutate(province_join = province_full |> str_squish() |> str_to_lower()),
                by = "province_join", copy = TRUE
            ) |>
            select(-province_join)
    } else {
        copy_to(statconn, metadata, name = "_m_tmp", overwrite = TRUE)
        metadata <- dbGetQuery(
            statconn,
            "
        SELECT m.*, b.name AS province_full
        FROM _m_tmp m
        LEFT JOIN boundary b
        ON ST_Contains(b.geom, ST_SetSRID(ST_MakePoint(lon, lat), 4326)) AND b.kind = 'province'
            "
        )
        dbRemoveTable(statconn, "_m_tmp")
        metadata |> associate_regional_info(NULL)
        # metadata |> mutate(province_full = NA_character_, province_code = NA_character_, state = NA_character_)
    }
}
