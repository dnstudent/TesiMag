library(purrr)
library(ggplot2)
library(stringr)
library(tibble)
library(tsibble)

source("load.R")


plot_series <- function(tvar, s1, s2, ...) {
    data <- load.series.named(tvar, s1, s2, ...) |>
        drop_na({{ tvar }}) |>
        fill_gaps({{ tvar }})
    # padr::pad("d", group = c("db", "station"))
    ggplot(data, aes(date, .data[[tvar]], color = station, linetype = db)) +
        geom_line() +
        labs(title = s1, subtitle = s2) +
        theme(plot.title = element_text(size = rel(0.8), color = "#ff5900"), plot.subtitle = element_text(size = rel(0.8), color = "#40d8e0"))
}
