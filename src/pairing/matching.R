library(sf, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(stars, warn.conflicts = FALSE)

source("src/pairing/metadata.R")


match_table <- function(metadata.x, metadata.y, dist_km = 5, dem = read_stars(file.path("temp", "dem", "dem30.tif")), asymmetric = FALSE) {
    if (asymmetric) {
        metadata.x <- metadata.x |>
            arrange(identifier) |>
            mutate(proxy_ident_ = row_number())
        metadata.y <- metadata.y |>
            arrange(identifier) |>
            mutate(proxy_ident_ = row_number())
    }
    matches <- st_join(
        metadata.x |> prepare_metadata(dem),
        metadata.y |> prepare_metadata(dem),
        left = FALSE,
        suffix = c(".x", ".y"),
        join = st_is_within_distance,
        dist = units::set_units(dist_km, "km")
    ) |>
        st_drop_geometry() |>
        filter(variable.x == variable.y) |>
        rename(variable = variable.x) |>
        select(-variable.y) |>
        add_distances(metadata.x, metadata.y) |>
        mutate(across(starts_with("identifier"), as.character), match_id = as.character(row_number()))
    if (asymmetric) {
        matches |>
            filter((last_date.x >= last_date.y) & (identifier.x != identifier.y)) |>
            select(!starts_with("proxy_ident_"))
    } else {
        matches
    }
}

widen_split_data <- function(data_ds, matches, identifier_var, first_date, last_date) {
    identifier_dtype <- schema(data_ds)$identifier$type
    semi_join(data_ds |> filter(first_date <= date, date <= last_date),
        matches |>
            select(identifier = {{ identifier_var }}, variable) |>
            as_arrow_table() |>
            mutate(identifier = cast(identifier, identifier_dtype)),
        by = join_by(variable, identifier)
    ) |>
        collect() |>
        arrange(variable) |>
        group_by(variable) |>
        group_map(
            ~ pivot_wider(.x, id_cols = date, names_from = identifier, values_from = value) |>
                arrange(date) |>
                as_tsibble(index = date) |>
                fill_gaps(.start = first_date, .end = last_date)
        )
}
