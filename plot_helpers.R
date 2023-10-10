library(purrr)
library(ggplot2)
library(stringr)
library(tibble)

source("load.R")


plot_series <- function(tvar, ...) {
    ggplot(load.series.named(tvar, ...), aes(date, .data[[tvar]], color = station, linetype = db)) +
        geom_line(na.rm = TRUE)
}
