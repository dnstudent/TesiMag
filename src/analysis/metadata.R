library(tsibble, warn.conflicts = FALSE)

source("src/load/load.R")
source("src/analysis/data/clim_availability.R")

build_date_stats <- function(db.metadata, db.data, variable) {
    dates <- drop_na(db.data |> as_tibble(), all_of(variable)) |>
        group_by(identifier) |>
        summarise(
            first_date = min(date),
            last_date = max(date),
            valid_days = n()
        ) |>
        ungroup()
    left_join(db.metadata, dates, by = "identifier")
}
