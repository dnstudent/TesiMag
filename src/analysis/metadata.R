library(tsibble, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

source("src/analysis/data/clim_availability.R")
source("src/database/write.R")

add_dem_elevations <- function(metadata, dem) {
    metadata |> mutate(dem = stars::st_extract(dem, as.matrix(bind_cols(lon = lon, lat = lat))) |> pull(1))
}

join_metas <- function(station_meta, series_meta, dem = stars::read_stars("temp/dem/dem30.tif")) {
    series_meta |>
        select(-any_of("merged_from")) |>
        left_join(station_meta, by = "station_id", relationship = "many-to-one") |>
        collect() |>
        add_dem_elevations(dem) |>
        as_arrow_table()
}

build_date_stats <- function(db.metadata, db.data, after = NULL, until = NULL) {
    if (!is.null(after)) {
        db.data <- filter(db.data, after <= date)
    }
    if (!is.null(until)) {
        db.data <- filter(db.data, date <= until)
    }
    db.data <- semi_join(db.data, db.metadata, by = c("variable", "identifier")) |> as_tibble()
    dates <- drop_na(db.data, value) |>
        group_by(variable, identifier) |>
        summarise(
            first_date = min(date),
            last_date = max(date),
            valid_days = n(),
            .groups = "drop"
        )
    left_join(db.metadata, dates, by = c("variable", "identifier"))
}

is_climatology_computable.metadata <- function(db.metadata, db.data, ...) {
    db.data |>
        semi_join(db.metadata, by = c("variable", "identifier")) |>
        group_by_key() |>
        is_climatology_computable.tbl_ts(value, ...)
}
