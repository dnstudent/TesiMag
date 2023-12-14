source("src/paths/paths.R")
source("src/database/tools.R")

path.veneto <- file.path(path.ds, "ARPA", "VENETO", "Dati_ARPAV_STAZIONI_AUTOMATICHE_al_02_02_2022")
path.tmin <- file.path(path.veneto, "ARPAV_Tmin.csv")
path.tmax <- file.path(path.veneto, "ARPAV_Tmax.csv")

load_original_table.arpav <- function(path, variable) {
    tab <- vroom::vroom(path, n_max = 8L, col_names = FALSE, show_col_types = FALSE) |> select(!X2:X4)
    col_names <- tab$X1
    ntab <- tab |>
        select(-X1) |>
        t()
    colnames(ntab) <- col_names
    # The stations crs is EPSG:3003 (probably)
    stations <- as_tibble(ntab) |>
        rename(original_id = statcd, station_name = statnm, elevation = altitude, province = provsgl, start_date = iniziovalidita, end_date = finevalidita) |>
        mutate(
            start_date = as.Date(start_date),
            end_date = na_if(end_date, "attiva") |> as.Date(),
            across(c(xkmpos, ykmpos, elevation), as.numeric),
            network = "ARPAV",
            state = state,
            dataset_id = dataset_id
        ) |>
        name_stations() |>
        assert(is_uniq, station_id) |>
        mutate(coords = sf_project(from = "EPSG:3003", to = "EPSG:4326", pick(xkmpos, ykmpos), authority_compliant = FALSE), lon = coords[, 1], lat = coords[, 2]) |>
        select(-coords)

    data <- vroom::vroom(path, skip = 9L, col_names = c("date", "year", "month", "day", stations$original_id), show_col_types = FALSE) |>
        select(!c(year, month, day)) |>
        pivot_longer(cols = !date, names_to = "original_id", values_to = "value") |>
        drop_na() |>
        filter(between(date, first_date, last_date)) |>
        arrange(as.numeric(original_id), date) |>
        left_join(stations |> select(original_id, station_id), by = "original_id") |>
        mutate(variable = variable, merged = FALSE) |>
        select(all_of(data_schema$names))

    if (nrow(data |> duplicates(key = c(station_id, variable), index = date)) > 0L) {
        stop("There were duplicates in the loaded data")
    }

    # Effectively filtering out stations that provide only old data
    stations <- stations |> semi_join(data, by = "station_id")

    # Asserting that every station has a series and vice versa
    full_join(stations, data, by = "station_id", relationship = "one-to-many") |>
        assert(not_na, c(network, variable))

    list("meta" = stations, "data" = data)
}

load_daily_data.arpav <- function() {
    tmin <- load_original_table.arpav(path.tmin, "T_MIN")
    tmax <- load_original_table.arpav(path.tmax, "T_MAX")

    # Checking that station identifiers point to the same station and there are no exceedings of stations
    full_join(tmin$meta, tmax$meta, by = "station_id") |>
        verify(station_name.x == station_name.y) |>
        verify(original_id.x == original_id.y) |>
        verify(lon.x == lon.y) |>
        verify(lat.x == lat.y) |>
        assert(not_na, starts_with("station_name"))

    data <- bind_rows(tmin$data, tmax$data)

    list("meta" = tmin$meta |> as_arrow_table(), "data" = data |> as_arrow_table2(data_schema))
}
