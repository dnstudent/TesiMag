library(arrow, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(rlang, warn.conflicts = FALSE)

source("src/database/definitions.R")

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
    base_meta <- select(full_station_list, all_of(station_schema$names)) |> as_arrow_table2(station_schema)
    extra_meta <- select(full_station_list, !all_of(station_schema$names), original_dataset, original_id) |> as_arrow_table()
    list("base" = base_meta, "extra" = extra_meta)
}

archive_path <- function(dataset, what, step) {
    file.path("db", what, dataset, paste0(step, ".parquet"))
}
