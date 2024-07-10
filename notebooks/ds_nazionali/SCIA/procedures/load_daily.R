library(dplyr, warn.conflicts = FALSE)
library(arrow, warn.conflicts = FALSE)
library(sf, warn.conflicts = FALSE)
library(assertr, warn.conflicts = FALSE)
library(stringi, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)

source("src/database/tools.R")
source("src/load/read/SCIA.R")
source("src/load/tools.R")
source("src/paths/paths.R")

dataset_spec <- function() {
    list(
        "http://193.206.192.214/servertsdailyutm/serietemporalidaily400.php",
        "national",
        "Dataset SCIA a passo giornaliero"
    )
}

load_meta <- function() {
    wfsreti_meta <- read.SCIA.metadata("T_MAX") |>
        rename(id = identifier, network = rete, name = anagrafica, district = state)
    official_meta <- vroom::vroom(
        file.path(path.ds, "SCIA", "stazioni", "stazioni_reduced.csv"),
        col_types = "cccddd",
        col_names = c("network", "name", "user_code", "lon", "lat", "elevation"), skip = 1L
    ) |>
        as_tibble() |>
        mutate(id = row_number())

    spatial_matches <- st_join(
        wfsreti_meta |> st_md_to_sf(),
        official_meta |> st_md_to_sf(),
        st_is_within_distance,
        dist = units::set_units(70, "m"),
        left = FALSE
    ) |> st_drop_geometry()
    exact_matches <- spatial_matches |>
        filter(user_code.x == user_code.y) |>
        assert(is_uniq, c(id.x, id.y))

    wfs_left <- wfsreti_meta |>
        anti_join(exact_matches, by = c("id" = "id.x"))
    off_left <- official_meta |>
        anti_join(exact_matches, by = c("id" = "id.y"))
    remaining_matches <- st_join(wfs_left |> st_md_to_sf(), off_left |> st_md_to_sf(), join = st_is_within_distance, dist = units::set_units(5, "km")) |>
        st_drop_geometry() |>
        mutate(strSym = stringdist::stringsim(
            str_to_lower(name.x) |> str_squish() |> stri_trans_general("Latin-ASCII"),
            str_to_lower(name.y) |> str_squish() |> stri_trans_general("Latin-ASCII"),
            method = "jw"
        )) |>
        filter(strSym > 0.9, abs(elevation.x - elevation.y) < 10) |>
        group_by(id.x) |>
        slice_min(abs(elevation.x - elevation.y), with_ties = FALSE) |>
        ungroup() |>
        select(all_of(colnames(exact_matches)))

    joined <- bind_rows(exact_matches, remaining_matches) |>
        select(
            id = id.x,
            network = network.y,
            name = name.y,
            user_code = user_code.y,
            lon = lon.y,
            lat = lat.y,
            elevation = elevation.y,
            last_year,
            first_year,
            valid_days,
            district,
            province,
            net_code
        )
    remaining <- wfs_left |> anti_join(remaining_matches, by = c("id" = "id.x"))
    bind_rows(joined, remaining)
}

load_daily_data.scia <- function() {
    meta <- load_meta() |>
        select(-valid_days) |>
        mutate(dataset = "SCIA", district = as.character(district), province = as.character(province), network = as.character(network), id = as.character(id), kind = "unknown") |>
        rename(series_id = id) |>
        as_arrow_table()

    tmin <- open_dataset(path.datafile("SCIA", "T_MIN")) |>
        rename(value = `Temperatura minima `) |>
        mutate(variable = "T_MIN")

    tmax <- open_dataset(path.datafile("SCIA", "T_MAX")) |>
        rename(value = `Temperatura massima `) |>
        mutate(variable = "T_MAX")

    data <- concat_tables(tmin |> compute(), tmax |> compute(), unify_schemas = FALSE) |>
        mutate(series_id = cast(internal_id, utf8()), .keep = "unused") |>
        # semi_join(meta, join_by(station_id == original_id)) |>
        filter(!is.na(value)) |>
        mutate(dataset = "SCIA") |>
        compute()

    meta <- meta |>
        mutate(
            station_id = NA_character_,
            sensor_id = NA_character_,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            station_first = as.Date(NA_integer_),
            station_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
            town = NA_character_,
        ) |>
        # semi_join(data, join_by(original_id == station_id)) |>
        compute()

    list("meta" = meta, "data" = data)
}
