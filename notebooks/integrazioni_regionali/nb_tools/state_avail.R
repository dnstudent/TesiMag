library(ggplot2, warn.conflicts = FALSE)
library(tsibble, warn.conflicts = FALSE)
options(repr.plot.width = 9, repr.plot.res = 300)

source("src/load/load.R")
source("src/analysis/data/clim_availability.R")

plot_state_avail <- function(series) {
    series |>
        as_tsibble(key = c(db, variable, identifier), index = date) |>
        fill_gaps(.full = TRUE) |>
        is_month_usable(value) |>
        index_by(year_month) |>
        summarise(n_available_series = sum(available)) |>
        ggplot(aes(year_month, n_available_series, color = db)) +
        geom_line() +
        facet_grid(variable ~ .)
}
