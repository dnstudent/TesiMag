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

load_original_table.arpav <- function(path, variable, first_date, last_date) {
    tab <- vroom::vroom(path, n_max = 8L, col_names = FALSE, show_col_types = FALSE) |> select(!X2:X4)
    col_names <- tab$X1
    ntab <- tab |>
        select(-X1) |>
        t()
    colnames(ntab) <- col_names
    # The stations crs is EPSG:3003 (probably)
    stations <- as_tibble(ntab) |>
        rename(id = statcd, name = statnm, elevation = altitude, province = provsgl, start_date = iniziovalidita, end_date = finevalidita) |>
        mutate(
            start_date = as.Date(start_date),
            end_date = na_if(end_date, "attiva") |> as.Date(),
            across(c(xkmpos, ykmpos, elevation), as.numeric),
            network = "ARPAV",
            state = state,
            dataset = "ARPAV",
            id = as.numeric(id)
        ) |>
        assert(is_uniq, id) |>
        mutate(coords = sf_project(from = "EPSG:3003", to = "EPSG:4326", pick(xkmpos, ykmpos), authority_compliant = FALSE), lon = coords[, 1], lat = coords[, 2]) |>
        select(-coords)

    data <- vroom::vroom(path, skip = 9L, col_names = c("date", "year", "month", "day", stations$id), show_col_types = FALSE) |>
        select(!c(year, month, day)) |>
        pivot_longer(cols = !date, names_to = "station_id", values_to = "value") |>
        drop_na() |>
        filter(between(date, first_date, last_date)) |>
        mutate(variable = variable, dataset = "ARPAV", station_id = as.numeric(station_id)) |>
        arrange(station_id, date) |>
        select(all_of(data_schema$names))

    if (nrow(data |> duplicates(key = c(dataset, station_id, variable), index = date)) > 0L) {
        stop("There were duplicates in the loaded data")
    }

    # Filtering out stations that provide only old data
    stations <- stations |> semi_join(data, join_by(dataset, id == station_id))

    # Asserting that every station has a series and vice versa
    full_join(stations, data, by = join_by(dataset, id == station_id), relationship = "one-to-many") |>
        assert(not_na, c(network, variable))

    list("meta" = stations, "data" = data)
}

load_daily_data.arpav <- function(first_date, last_date) {
    tmin <- load_original_table.arpav(path.tmin, "T_MIN", first_date, last_date)
    tmax <- load_original_table.arpav(path.tmax, "T_MAX", first_date, last_date)

    # Checking that station identifiers point to the same station and there are no exceedings of stations
    full_join(tmin$meta, tmax$meta, by = "id") |>
        verify(name.x == name.y) |>
        verify(lon.x == lon.y) |>
        verify(lat.x == lat.y) |>
        assert(not_na, starts_with("name"))

    data <- bind_rows(tmin$data, tmax$data)

    list("meta" = tmin$meta |> as_arrow_table(), "data" = data |> as_arrow_table2(data_schema))
}
