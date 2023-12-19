library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/load/tools.R")
source("src/database/tools.R")

filter_checkpoint_inside <- function(database, region) {
    if (is.null(region)) {
        return(database)
    }
    meta <- database$meta |>
        collect() |>
        st_md_to_sf(remove = FALSE) |>
        sf::st_filter(region, .predicate = sf::st_within) |>
        sf::st_drop_geometry() |>
        as_arrow_table2(station_schema)
    list(
        "meta" = meta,
        "data" = database$data |> semi_join(meta, by = "station_id")
    )
}

concat_databases <- function(database.x, database.y, check = TRUE) {
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
