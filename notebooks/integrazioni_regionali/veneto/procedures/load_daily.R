source("src/paths/paths.R")
source("src/database/tools.R")

dataset_spec <- function() {
    list(
        "locale",
        "regional",
        "Dataset delle stazioni automatiche di ARPA Veneto. Fornito come csv da Maugeri. Interrotto a fine 2021."
    )
}

path.veneto <- file.path(path.ds, "ARPA", "VENETO", "Dati_ARPAV_STAZIONI_AUTOMATICHE_al_02_02_2022")
path.tmin <- file.path(path.veneto, "ARPAV_Tmin.csv")
path.tmax <- file.path(path.veneto, "ARPAV_Tmax.csv")

load_original_table.arpav <- function(path, variable) {
    tab <- vroom::vroom(path, n_max = 8L, col_names = FALSE, show_col_types = FALSE) |>
        select(!X2:X4) |>
        as_tibble()
    col_names <- tab$X1
    ntab <- tab |>
        select(-X1) |>
        t()
    colnames(ntab) <- col_names
    # The stations crs is EPSG:3003 (probably)
    stations <- as_tibble(ntab) |>
        rename(original_id = statcd, name = statnm, elevation = altitude, province = provsgl, start_date = iniziovalidita, end_date = finevalidita) |>
        mutate(
            start_date = as.Date(start_date),
            end_date = na_if(end_date, "attiva") |> as.Date(),
            across(c(xkmpos, ykmpos, elevation), as.double),
            network = "ARPAV",
            state = "Veneto",
            original_dataset = "ARPAV",
            original_id = as.integer(original_id),
            kind = "unknown"
        ) |>
        assert(is_uniq, original_id) |>
        mutate(coords = sf_project(from = "EPSG:3003", to = "EPSG:4326", pick(xkmpos, ykmpos), authority_compliant = FALSE), lon = coords[, 1], lat = coords[, 2]) |>
        select(-coords)

    data <- vroom::vroom(path, skip = 9L, col_names = c("date", "year", "month", "day", stations$original_id), show_col_types = FALSE) |>
        as_tibble() |>
        select(!c(year, month, day)) |>
        pivot_longer(cols = !date, names_to = "station_id", values_to = "value") |>
        drop_na() |>
        mutate(variable = variable, dataset = "ARPAV", station_id = as.integer(station_id)) |>
        arrange(station_id, date) |>
        select(all_of(data_schema$names))

    if (nrow(data |> duplicates(key = c(dataset, station_id, variable), index = date)) > 0L) {
        stop("There were duplicates in the loaded data")
    }

    # Asserting that every station has a series and vice versa
    full_join(stations, data, by = join_by(original_id == station_id), relationship = "one-to-many") |>
        assert(not_na, c(network, variable))

    list("meta" = stations, "data" = data)
}

load_daily_data.arpav <- function() {
    best_meta <- vroom::vroom(file.path(path.ds, "ARPA", "VENETO", "metadata.csv"), col_types = "iicdddccc", show_col_types = F) |>
        as_tibble() |>
        rename(lat = latitudine, lon = longitudine, elevation = altitude, original_id = codice_stazione, name = nome_stazione, province = provincia)

    tmin <- load_original_table.arpav(path.tmin, "T_MIN")
    tmax <- load_original_table.arpav(path.tmax, "T_MAX")

    meta_upd <- tmin$meta |>
        select(!any_of(colnames(best_meta)), original_id) |>
        inner_join(best_meta, by = "original_id")

    meta <- bind_rows(
        meta_upd,
        tmin$meta |> anti_join(meta_upd, by = "original_id"),
    ) |>
        assert(not_na, starts_with("name")) |>
        select(!ends_with("date"))


    data <- bind_rows(tmin$data, tmax$data)

    list("meta" = meta |> as_arrow_table(), "data" = data |> as_arrow_table())
}
