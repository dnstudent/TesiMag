library(dplyr, warn.conflicts = FALSE)
library(ggpubr, warn.conflicts = FALSE)

source("src/plot/plot_helpers.R")

comp_plots <- function(paired, class_var, n = 5, start_date = "2010-01-01") {
    plots <- paired |>
        filter(class == class_var) |>
        sample_n(size = n) |>
        select(variable, starts_with("identifier"), anagrafica.x) |>
        rowwise() |>
        group_map(~ plot.sciavsdpc(.x[["variable"]], .x[["identifier.x"]], .x[["identifier.y"]], "qc_era5", start_date = start_date, anagrafica.scia = .x[["anagrafica.x"]]))
    ggarrange(plotlist = plots, ncol = 1)
}
