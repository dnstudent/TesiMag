source("load.BRUN.R")

load.DPC.series.single <- load.BRUN.series.single
load.DPC._series <- function(tvar, which_one) {
    load.brun.__series("DPC", tvar, NULL)
}

load.DPC._metadata <- function(tvar) {
    load.BRUN._metadata(tvar) |> filter(DPC)
}
