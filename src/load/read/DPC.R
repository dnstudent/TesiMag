source("src/paths/paths.R")
source("src/load/read/brunetti.R")

read.DPC.series.single <- \(tvar, identifier, flavor) read.brunetti.series.single("DPC", tvar, flavor, identifier)
read.DPC.series.bunch <- \(tvar, identifiers, flavor) read.brunetti.series("DPC", tvar, flavor, identifiers)

read.DPC.series <- function(tvar, flavor) {
    read.brunetti.series("DPC", tvar, flavor, NULL)
}

read.DPC.metadata <- function(tvar, flavor) {
    read.brunetti.metadata.raw_("DPC", tvar, flavor) |>
        filter(region_ == "dpc") |>
        describe.brunetti.region_dpc_() |>
        mutate(
            across(where(is.character), \(x) na_if(x, "")),
            province = as.factor(province)
        ) |>
        left_join(italian_districts_, join_by(district == code)) |>
        select(-district) |>
        mutate(district = as.factor(name), .keep = "unused")
}
