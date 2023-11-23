library(tsibble, warn.conflicts = FALSE)

source("src/load/load.R")
source("src/analysis/data/clim_availability.R")

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
