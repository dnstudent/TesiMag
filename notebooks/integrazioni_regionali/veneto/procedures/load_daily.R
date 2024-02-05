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
        rename(station_id = statcd, name = statnm, elevation = altitude, province_code = provsgl, station_first = iniziovalidita, station_last = finevalidita) |>
        mutate(
            station_first = as.Date(station_first),
            station_last = na_if(station_last, "attiva") |> as.Date(),
            across(c(xkmpos, ykmpos, elevation), as.double),
            network = "ARPAV",
            dataset = "ARPAV",
            station_id = as.integer(station_id),
            kind = "unknown"
        ) |>
        assert(is_uniq, station_id) |>
        mutate(coords = sf_project(from = "EPSG:3003", to = "EPSG:4326", pick(xkmpos, ykmpos), authority_compliant = FALSE), lon = coords[, 1], lat = coords[, 2]) |>
        select(-coords)

    data <- vroom::vroom(path, skip = 9L, col_names = c("date", "year", "month", "day", stations$station_id), show_col_types = FALSE) |>
        as_tibble() |>
        select(!c(year, month, day)) |>
        pivot_longer(cols = !date, names_to = "station_id", values_to = "value") |>
        drop_na() |>
        mutate(variable = !!variable, dataset = "ARPAV", station_id = as.integer(station_id)) |>
        arrange(station_id, date)

    if (nrow(data |> duplicates(key = c(dataset, station_id, variable), index = date)) > 0L) {
        stop("There are duplicates in the loaded data")
    }

    # Asserting that every station has a series and vice versa
    full_join(stations, data, by = "station_id", relationship = "one-to-many") |>
        assert(not_na, c(network, variable))

    list("meta" = stations, "data" = data)
}

load_daily_data.arpav <- function() {
    best_meta <- vroom::vroom(file.path(path.ds, "ARPA", "VENETO", "metadata.csv"), col_types = "iicdddccc", show_col_types = F) |>
        as_tibble() |>
        rename(lat = latitudine, lon = longitudine, elevation = altitude, station_id = codice_stazione, name = nome_stazione, province_full = provincia, user_code = codseq, town = comune)

    tmin <- load_original_table.arpav(path.tmin, "T_MIN")
    tmax <- load_original_table.arpav(path.tmax, "T_MAX")

    meta_upd <- tmin$meta |>
        select(!any_of(colnames(best_meta)), station_id) |>
        inner_join(best_meta, by = "station_id") |>
        compute()

    meta <- bind_rows(
        meta_upd,
        tmin$meta |> anti_join(meta_upd, by = "station_id"),
    ) |>
        assert(not_na, starts_with("name")) |>
        select(!ends_with("date")) |>
        mutate(
            sensor_id = NA_character_,
            station_id = as.character(station_id),
            series_id = station_id,
            sensor_first = as.Date(NA_integer_),
            sensor_last = as.Date(NA_integer_),
            series_first = as.Date(NA_integer_),
            series_last = as.Date(NA_integer_),
        ) |>
        compute()


    data <- bind_rows(tmin$data, tmax$data) |>
        mutate(station_id = as.character(station_id)) |>
        compute()

    list("meta" = meta |> as_arrow_table(), "data" = data |> as_arrow_table())
}
