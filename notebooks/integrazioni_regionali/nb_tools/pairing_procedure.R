library(arrow, warn.conflicts = FALSE)
library(stars, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("notebooks/integrazioni_regionali/nb_tools/state_avail.R")
source("src/load/load.R")
source("src/analysis/data/clim_availability.R")
source("src/pairing/analysis.R")
source("src/pairing/matching.R")
source("src/pairing/plots.R")
source("src/pairing/combining.R")
source("src/pairing/displaying.R")

plot_availabilities <- function(data.x, data.y) {
    plot_state_avail(bind_rows(
        db1 = data.x |> mutate(identifier = as.character(identifier)),
        db2 = data.y |> mutate(identifier = as.character(identifier)),
        .id = "db"
    ))
}


load_scia_metadata <- function(buffer, dem = read_stars("temp/dem/dem30.tif")) {
    open.dataset("SCIA", "metadata") |>
        collect() |>
        st_md_to_sf() |>
        st_filter(buffer, .predicate = st_within) |>
        prepare_metadata(dem)
}

load_dpc_metadata <- function(buffer, dem = read_stars("temp/dem/dem30.tif")) {
    open.dataset("BRUN", "metadata") |>
        filter(flavor == "qc_era5") |>
        collect() |>
        st_md_to_sf() |>
        st_filter(buffer, .predicate = st_within) |>
        prepare_metadata(dem)
}

load_scia_data <- function(metadata) {
    open.dataset("SCIA", "data") |>
        semi_join(metadata, by = c("variable", "identifier"))
}

build_analysis <- function(matches, data.x.tmin, data.x.tmax, data.y.tmin, data.y.tmax) {
    bind_rows(
        T_MIN = analyze_matches(
            matches |> filter(variable == "T_MIN"),
            data.x.tmin,
            data.y.tmin
        ),
        T_MAX = analyze_matches(
            matches |> filter(variable == "T_MAX"),
            data.x.tmax,
            data.y.tmax
        ),
        .id = "variable"
    )
}

plot_clim_availability <- function(clim_availablility, metadata) {
    clim_availablility |>
        as_tibble() |>
        group_by(variable, identifier) |>
        summarise(all = all(clim_available), .groups = "drop") |>
        left_join(metadata |> group_by(identifier) |> slice_tail() |> select(identifier), by = "identifier") |>
        st_as_sf() |>
        ggplot() +
        geom_sf(aes(color = all)) +
        facet_grid(~variable)
}

n_available_series <- function(data, start_date, end_date) {
    is_climatology_computable(data |> group_by_key(), value, start_date, end_date) |>
        as_tibble() |>
        group_by(variable, identifier) |>
        summarise(all = all(clim_available), .groups = "drop") |>
        filter(all) |>
        nrow()
}

merged_data <- function(matchlist, data.x.tmin, data.x.tmax, data.y.tmin, data.y.tmax) {
    merged <- bind_rows(
        T_MIN = update_left(matchlist |> filter(variable == "T_MIN"), data.x.tmin, data.y.tmin),
        T_MAX = update_left(matchlist |> filter(variable == "T_MAX"), data.x.tmax, data.y.tmax),
        .id = "variable"
    ) |>
        select(-correction) |>
        left_join(matchlist |> select(variable, identifier.x, identifier.y, match_id), by = c("variable", "match_id"), relationship = "many-to-one")
}

arpa_schema <- schema(
    identifier.x = string(),
    db.x = string(),
    date = date32(),
    value = float(),
    from.y = bool(),
    variable = string(),
    reference = string(),
)
merged_ds_schema <- schema(
    identifier.x = string(),
    identifier.y = string(),
    db.x = string(),
    db.y = string(),
    date = date32(),
    value = float(),
    from.y = bool(),
    variable = string(),
    reference = string(),
)

write_arpa_for_merge <- function(data, reference) {
    data |>
        arrange(variable, identifier, date) |>
        rename(identifier.x = identifier) |>
        mutate(identifier.y = NA_character_, db.x = reference, db.y = NA_character_, from.y = FALSE, reference = reference) |>
        relocate(identifier.x, identifier.y, db.x, db.y, date, value, from.y, variable, reference) |>
        as_arrow_table(schema = merged_ds_schema) |>
        write_feather(file.path("db", "pieces", paste0(reference, ".feather")))
}

open_db_state_data <- function(db, state.name) {
    md <- open.dataset(db, "metadata") |> filter(state == state.name)
    if (db == "DPC" || db == "BRUN") {
        md <- filter(md, flavor == "qc_era5")
        ds <- open.dataset(db, "data") |>
            semi_join(md, by = c("flavor", "variable", "identifier"))
    } else {
        ds <- open.dataset(db, "data") |>
            semi_join(md, by = c("variable", "identifier"))
    }
    ds |>
        mutate(identifier = cast(identifier, utf8())) |>
        collect()
}
