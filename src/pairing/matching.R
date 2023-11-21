library(sf, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(RcppRoll, warn.conflicts = FALSE)

source("src/pairing/analysis.R")

matches_table <- function(metadata.x, metadata.y, dist_km = 5) {
    st_join(
        metadata.x,
        metadata.y,
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
}

widen_split_data <- function(data_ds, matches, identifier_var, first_date = as.Date("2000-01-01"), last_date = as.Date("2022-12-31")) {
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
