library(dplyr)
library(stringr)
library(tidyr)
library(tidyselect)
library(vroom)
library(tsibble)

source("paths.R")
regions <- list(
    dpc = regex("^T[XN]_([[:alpha:]]{3})_([[:alpha:]]{2})_([[:alnum:]_]+)_([[:digit:]]{2})_([[:digit:]]{9})"),
    at = regex("^TM[XN]D_AT_"),
    ita = regex("^TM[XN]D_ITA?_")
    # simple = regex("^TM[XN]D_[:upper:]*(?:[:digit:]{6})?(?:_MG)?"),
)
classify_region <- function(identifier) {
    case_when(
        str_detect(identifier, regions$dpc) ~ "dpc",
        str_detect(identifier, regions$at) ~ "at",
        str_detect(identifier, regions$ita) ~ "ita",
        .default = "simple"
    ) |> as.factor()
}


# load.BRUN.region_dpc <- function(fpath, first = NULL, last = NULL) {
#     if (is.null(last)) {
#         if (!is.null(first)) {
#             last <- first
#         } else {
#             last <- Inf
#         }
#         first <- 1
#     }
#     widths.region_full <- c(86, 9, 12, 7, 10)
#     vroom_fwf(fpath, fwf_widths(widths.region_full, col_names = c("identifier", "lon", "lat", "elevation", "internal_id")), col_types = cols(identifier = "c", internal_id = "i", .default = "d"), skip = first - 1, n_max = last - first + 1)
# }

mutate.BRUN.region_dpc <- function(data) {
    data |>
        separate_wider_regex(identifier, c("T[XN]_", state = "[:alpha:]{3}", "_", province = "[:alpha:]{2}", "_", anagrafica = "[[:alnum:]_]+", "_", version = "[:digit:]{2}", "_", internal_id = "[:digit:]{9}", "\\.?[:alpha:]*"), cols_remove = FALSE) |>
        mutate(across(c(state, province), as.factor),
            version = as.integer(version),
            internal_id = as.integer(internal_id),
            state = na_if(state, "XXX"),
            province = na_if(province, "XX")
        )
}

# load.BRUN.region_ssv <- function(fpath, first, last = Inf) {
#     vroom(
#         fpath,
#         delim = " ",
#         col_names = c("identifier", "lon", "lat", "elevation"),
#         col_types = cols(identifier = "c", .default = "d"),
#         skip = first - 1, n_max = last - first + 1
#     )
# }

mutate.BRUN.region_simple <- function(data) {
    data |>
        separate_wider_regex(identifier, c("TM[XN]D_", anagrafica = "[:upper:]*", user_code = "(?:[:digit:]{6})?", MG = "(?:_MG)?", "(?:\\.[:lower:]+)?"), cols_remove = FALSE) |>
        mutate(MG = str_detect(MG, "_MG"))
}

mutate.BRUN.region_at <- function(data) {
    data |>
        separate_wider_regex(identifier, c("TM[XN]D_", country = "[:alpha:]{2}", "_", anagrafica = "[[:upper:]_]+?", GSOD = "(?:_GSOD)?", "_", user_code = "[:digit:]{6}", MG = "(?:_MG)?", "(?:\\.[:lower:]+)?"), cols_remove = FALSE) |>
        mutate(GSOD = str_detect(GSOD, "_GSOD"), MG = str_detect(MG, "_MG"))
}

mutate.BRUN.region_ita <- function(data) {
    data |>
        separate_wider_regex(identifier, c("TM[XN]D_", country = "[:alpha:]{2,3}", "_", state = "[:alpha:]{3}", "_", province = "[:alpha:]{2}", "_", anagrafica = "[[:upper:]_[:digit:]-]*?", "_?", user_code = "t[:digit:]{4}|(?:S|N)?[:digit:]{3,4}|[:digit:]{5}[:upper:]{2}|", MG = "(?:_MG)?", "(?:\\.[:lower:]+$)?"), cols_remove = FALSE) |>
        mutate(
            MG = str_detect(MG, "_MG"),
            state = na_if(state, "XXX"),
            province = na_if(province, "XX")
        )
}

italian_states <- tibble(
    code = c("ABR", "BAS", "CAL", "CAM", "EMR", "FVG", "LAZ", "LIG", "LOM", "MAR", "MOL", "PIE", "PUG", "SAR", "SIC", "TOS", "TAA", "UMB", "VDA", "VEN"),
    name = c("Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", "Friuli-Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche", "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", "Trentino-Alto Adige", "Umbria", "Valle D'Aosta", "Veneto")
)

.BRUN.metadata_ <- function(tvar, which_one, verbose = FALSE) {
    lines <- path.anagrafica("BRUN", tvar, which_one) |> readLines()
    suppressWarnings(vroom(
        I(lines |> str_squish()),
        delim = " ",
        col_names = c("identifier", "lon", "lat", "elevation"),
        col_select = 1:4,
        show_col_types = FALSE
    ) |> mutate(region = classify_region(identifier)))
}

process.BRUN.metadata_ <- function(data) {
    # Probably can be done automagically through group_by and metaprogramming. It shurely would be faster.
    data |>
        group_by(region) |>
        group_modify(~ do.call(paste0("mutate.BRUN.region_", .y[[1]]), list(.x)), .keep = FALSE) |>
        ungroup() |>
        mutate(
            GSOD = replace_na(GSOD, FALSE),
            MG = replace_na(MG, FALSE),
            country = replace_na(country, "ITA"),
            across(where(is.character), \(x) na_if(x, "")),
            country = as.factor(country),
            state = as.factor(state),
            province = as.factor(province)
        ) |>
        left_join(italian_states, join_by(state == code)) |>
        select(-state) |>
        rename(state = name)
}

load.BRUN.metadata_ <- function(tvar, which_one) {

}
# load.BRUN._metadata <- function(tvar, which_one) {
#     fpath <- path.anagrafica("BRUN", tvar, which_one)
#     data <- bind_rows(
#         load.BRUN.region_dpc(fpath, 3737) |> mutate(DPC = TRUE),
#         load.BRUN.region_ssv(fpath, 3738) |> mutate(DPC = FALSE)
#     )
#     if (tvar == "T_MIN") {
#         data <- bind_rows(
#             data |> slice(1:3737) |> mutate.BRUN.region_dpc(),
#             data |> slice(3738:3868, 3897:3902, 4119:4134) |> mutate.BRUN.region_simple(),
#             data |> slice(3869:3896) |> mutate.BRUN.region_at(),
#             data |> slice(3903:4118) |> mutate.BRUN.region_ita()
#         )
#     } else if (tvar == "T_MAX") {
#         data <- bind_rows(
#             data |> slice(1:3737) |> mutate.BRUN.region_dpc(),
#             data |> slice(3738:3868, 3897:3902, 4126:4141) |> mutate.BRUN.region_simple(),
#             data |> slice(3869:3896) |> mutate.BRUN.region_at(),
#             data |> slice(3903:4125) |> mutate.BRUN.region_ita()
#         )
#     } else {
#         simpleError("Invalid tvar")
#     }
#     data |>
#         mutate(
#             GSOD = replace_na(GSOD, FALSE),
#             MG = replace_na(MG, FALSE),
#             across(where(is.character), \(x) na_if(x, "")),
#             country = as.factor(country),
#             state = as.factor(state),
#             province = as.factor(province)
#         ) |>
#         left_join(italian_states, join_by(state == code)) |>
#         select(-state) |>
#         rename(state = name)
# }
#
widths.series <- c(5, 3, rep(7, 31))
load.brun.__series <- function(db, tvar, which_one, identifier) {
    if (is.null(identifier)) {
        path_s <- path.series(db, tvar, which_one, check = TRUE)
    } else {
        path_s <- file.path(path.section(db, tvar, which_one), identifier)
    }
    vroom::vroom_fwf(path_s,
        fwf_widths(widths.series, col_names = c("year", "month", seq.int(1, 31))),
        col_types = cols(year = "i", month = "i", .default = "d"),
        na = c("", "NA", "-90.0"), altrep = FALSE, id = "file", progress = FALSE
    ) |>
        mutate(identifier = factor(str_split_i(file, "/", -1)), .keep = "unused", .before = 1) |>
        pivot_longer(cols = seq(4, 34), names_to = "day", values_to = tvar, names_transform = as.integer) |>
        mutate(date = make_date(year, month, day), .keep = "unused") |>
        filter(!is.na(date)) |>
        rename(identifier = identifier)
}

load.BRUN.series.single <- function(tvar, identifier, which_one) {
    load.brun.__series("BRUN", tvar, which_one, identifier)
}

load.BRUN._series <- function(tvar, which_one) {
    load.brun.__series("BRUN", tvar, which_one, NULL)
}
