library(purrr, warn.conflicts = FALSE)
library(ggplot2, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(tsibble, warn.conflicts = FALSE)

source("src/load/load.R")


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

plot.sciavsdpc <- function(tvar, id.scia, id.dpc, flavor.dpc) {
    s.scia <- read.series.single("SCIA", tvar, id.scia) |>
        drop_na({{ tvar }}) |>
        fill_gaps()
    s.dpc <- read.series.single("DPC", tvar, id.dpc, flavor = flavor.dpc) |>
        drop_na({{ tvar }}) |>
        fill_gaps()
    data <- list(s.scia, s.dpc)
    data <- bind_rows(list(s.scia |> as_tibble(), s.dpc |> as_tibble()) |> setNames(c(id.scia, id.dpc)), .id = "station")
    ggplot(data) +
        geom_line(aes(date, .data[[tvar]], color = station, linetype = station))
}
