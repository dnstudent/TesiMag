library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/load/tools.R")
source("src/database/tools.R")

filter_inside <- function(metadata, boundaries) {
    metadata |>
        collect() |>
        st_md_to_sf() |>
        sf::st_filter(boundaries, .predicate = sf::st_within) |>
        st::st_drop_geometry() |>
        select(dataset, id) |>
        as_arrow_table()
}

filter_checkpoint_inside <- function(checkpoint, region) {
    if (is.null(region)) {
        return(checkpoint)
    }
    meta <- checkpoint$meta |>
        collect() |>
        st_md_to_sf(remove = FALSE) |>
        sf::st_filter(region, .predicate = sf::st_within) |>
        sf::st_drop_geometry() |>
        as_arrow_table2(station_schema)
    list(
        "meta" = meta,
        "data" = checkpoint$data |> semi_join(meta, join_by(dataset == original_dataset, station_id == original_id)) |> compute()
    )
}

concat_databases <- function(database.x, database.y, check = FALSE) {
    meta <- concat_tables(database.x$meta |> compute(), database.y$meta |> compute(), unify_schemas = FALSE)
    data <- concat_tables(database.x$data |> compute(), database.y$data |> compute(), unify_schemas = FALSE)
    if (check) {
        meta |> assert(is_uniq, station_id)
        if (data |> group_by(station_id, variable, date) |> tally() |> filter(n > 1L) |> compute() |> nrow() > 0L) {
            stop("Combined data is not unique in station_id, variable, date")
        }
    }

    list(
        "meta" = meta,
        "data" = data
    )
}
