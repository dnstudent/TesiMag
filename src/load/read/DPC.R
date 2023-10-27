source("src/paths/paths.R")
source("src/load/read/brunetti.R")

read.DPC.series.single <- \(tvar, flavor, identifier) read.brunetti.series.single("DPC", tvar, flavor, identifier)

read.DPC.series <- function(tvar, flavor) {
    read.brunetti.series("DPC", tvar, flavor, NULL)
}

read.DPC.metadata <- function(tvar, flavor) {
    read.brunetti.metadata.raw_("DPC", tvar, flavor) |>
        filter(region_ == "dpc") |>
        describe.brunetti.region_dpc_()
}
