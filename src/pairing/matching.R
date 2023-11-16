library(sf, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/pairing/analysis.R")

matches_table <- function(stations.scia, stations.dpc) {
    st_join(
        stations.scia,
        stations.dpc,
        left = FALSE,
        suffix = c(".x", ".y"),
        join = st_is_within_distance,
        dist = units::set_units(5, "km")
    ) |>
        filter(variable.x == variable.y) |>
        group_by(variable.x, identifier.x) |>
        mutate(matches.x = n()) |>
        ungroup() |>
        group_by(variable.y, identifier.y) |>
        mutate(matches.y = n()) |>
        ungroup() |>
        add_distances(stations.scia, stations.dpc) |>
        rename(variable = variable.x) |>
        select(-variable.y) |>
        st_drop_geometry()
}

prepare_dss <- function(ds, matches_table, identifier_var, cast_dtype) {
    data <- semi_join(ds |> filter(first_date <= date, date <= last_date),
        matches_table |>
            select(identifier = {{ identifier_var }}, variable) |>
            as_arrow_table() |>
            mutate(identifier = cast(identifier, cast_dtype)),
        by = join_by(variable, identifier)
    ) |>
        collect() |>
        arrange(variable) |>
        group_by(variable) |>
        group_map(
            ~ pivot_wider(.x, id_cols = date, names_from = identifier, values_from = value) |>
                arrange(date) |>
                as_tsibble(index = date) |>
                fill_gaps(.start = first_date, .end = last_date) |>
                as_tibble()
        )

    means <- purrr::map(
        data,
        ~ . |>
            group_by(month = month(date), year = year(date)) |>
            summarise(across(-date, ~ mean(., na.rm = TRUE)), .groups = "drop_last") |>
            summarise(across(-year, ~ mean(., na.rm = TRUE)), .groups = "drop")
    )

    list(data, means)
}
