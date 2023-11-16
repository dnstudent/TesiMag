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

plot.sciavsdpc <- function(tvar, id.scia, id.dpc, flavor.dpc, diffs = TRUE, monthly = TRUE, start_date = NULL, anagrafica.scia = NULL) {
    s.scia <- read.series.single("SCIA", tvar, id.scia) |>
        drop_na({{ tvar }}) |>
        fill_gaps() |>
        rename(T = {{ tvar }})
    s.dpc <- read.series.single("DPC", tvar, id.dpc, flavor = flavor.dpc) |>
        drop_na({{ tvar }}) |>
        fill_gaps() |>
        rename(T = {{ tvar }})
    if (!is.null(start_date)) {
        s.scia <- filter(s.scia, date >= as.Date(start_date))
        s.dpc <- filter(s.dpc, date >= as.Date(start_date))
    }
    if (monthly) {
        s.scia <- s.scia |>
            index_by(ymt = ~ yearmonth(.)) |>
            summarise(T = mean(T, na.rm = TRUE)) |>
            rename(date = ymt)
        s.dpc <- s.dpc |>
            index_by(ymt = ~ yearmonth(.)) |>
            summarise(T = mean(T, na.rm = TRUE)) |>
            rename(date = ymt)
    }
    if (diffs) {
        inner_join(s.scia, s.dpc, by = "date") |>
            mutate(
                diffs = T.x - T.y,
                nas = case_when(
                    is.na(T.x) & is.na(T.y) ~ "both",
                    is.na(T.x) ~ "scia",
                    is.na(T.y) ~ "dpc",
                    .default = "none"
                ) |> factor(levels = c("none", "scia", "dpc", "both"))
            ) |>
            ggplot() +
            geom_rect(aes(NULL, NULL, xmin = date, xmax = lag(date), fill = nas), ymin = -0.5, ymax = 0.5, na.rm = TRUE) +
            scale_fill_manual(values = c(alpha("white", 0), alpha(c("red", "blue", "green"), 0.8))) +
            geom_line(aes(date, diffs), na.rm = TRUE) +
            labs(title = str_glue("{id.scia}({anagrafica.scia}) vs {id.dpc}"))
    } else {
        data <- list(s.scia, s.dpc)
        data <- bind_rows(list(s.scia |> as_tibble(), s.dpc |> as_tibble()) |> setNames(c(id.scia, id.dpc)), .id = "station")
        ggplot(data) +
            geom_line(aes(date, T, color = station, linetype = station))
    }
}
