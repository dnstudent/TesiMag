library(dplyr)
library(stringr)
library(tidyr)
library(tidyselect)
library(forcats)

source("src/paths/paths.R")
source("src/load/read/brunetti.R")

italian_states_ <- tibble(
    code = c("ABR", "BAS", "CAL", "CAM", "EMR", "FVG", "LAZ", "LIG", "LOM", "MAR", "MOL", "PIE", "PUG", "SAR", "SIC", "TOS", "TAA", "UMB", "VDA", "VEN"),
    name = c("Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", "Friuli-Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche", "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", "Trentino-Alto Adige", "Umbria", "Valle D'Aosta", "Veneto")
)

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


read.BRUN.series.single <- function(tvar, flavor, identifier) {
    read.brunetti.series("BRUN", tvar, flavor, identifier)
}

read.BRUN.series <- function(tvar, flavor) {
    read.brunetti.series("BRUN", tvar, flavor, NULL)
}
