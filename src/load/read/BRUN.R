library(dplyr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(tidyselect, warn.conflicts = FALSE)
library(forcats, warn.conflicts = FALSE)

source("src/paths/paths.R")
source("src/load/read/brunetti.R")



describe.BRUN.metadata_ <- function(data) {
    data |>
        group_by(region_) |>
        group_modify(~ do.call(paste0("describe.brunetti.region_", .y[[1]], "_"), list(.x)), .keep = FALSE) |>
        ungroup() |>
        mutate(
            across(where(is.character), \(x) na_if(x, "")),
            GSOD = replace_na(GSOD, FALSE),
            MG = replace_na(MG, FALSE),
            country = replace_na(country, "IT") |> as.factor() |> fct_collapse(IT = c("IT", "ITA")),
            state = as.factor(state),
            province = as.factor(province)
        ) |>
        left_join(italian_states_, join_by(state == code)) |>
        select(-state) |>
        rename(state = name)
}

read.BRUN.metadata <- function(tvar, flavor) {
    read.brunetti.metadata.raw_("BRUN", tvar, flavor) |> describe.BRUN.metadata_()
}


read.BRUN.series.single <- function(tvar, identifier, flavor) {
    read.brunetti.series.single("BRUN", tvar, flavor, identifier)
}

read.BRUN.series.bunch <- function(tvar, identifiers, flavor) {
    read.brunetti.series("BRUN", tvar, flavor, identifiers)
}

read.BRUN.series <- function(tvar, flavor) {
    read.brunetti.series("BRUN", tvar, flavor, NULL)
}
