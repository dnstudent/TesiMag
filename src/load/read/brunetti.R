library(vroom, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)
library(tibble, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/paths/paths.R")

regions_ <- list(
    dpc = regex("^T[XN]_[[:alpha:]]{3}_[[:alpha:]]{2}_[[:alnum:]_]+_[[:digit:]]{2}_[[:digit:]]{9}"),
    at = regex("^TM[XN]D_AT_"),
    ita = regex("^TM[XN]D_ITA?_")
    # simple = regex("^TM[XN]D_[:upper:]*(?:[:digit:]{6})?(?:_MG)?"),
)
classify_region_ <- function(identifier) {
    case_when(
        str_detect(identifier, regions_$dpc) ~ "dpc",
        str_detect(identifier, regions_$at) ~ "at",
        str_detect(identifier, regions_$ita) ~ "ita",
        .default = "simple"
    ) |> as.factor()
}

read.brunetti.metadata.raw_ <- function(db, tvar, flavor) {
    data <- path.metadata(db, tvar, flavor) |>
        readLines() |>
        str_squish() |>
        str_split(" ") |>
        sapply(\(l) l[1:4]) |>
        t()
    colnames(data) <- c("identifier", "lon", "lat", "elevation")
    as_tibble(data) |>
        mutate(
            lon = as.double(lon),
            lat = as.double(lat),
            elevation = as.double(elevation),
            region_ = classify_region_(identifier),
            identifier = str_split_i(identifier, fixed("."), 1)
        )
}

describe.brunetti.region_dpc_ <- function(data) {
    data |>
        separate_wider_regex(identifier, c("T[XN]_", district = "[:alpha:]{3}", "_", province = "[:alpha:]{2}", "_", anagrafica = "[[:alnum:]_]+", "_", version = "[:digit:]{2}", "_", internal_id = "[:digit:]{9}", "\\.?[:alpha:]*"), cols_remove = FALSE) |>
        mutate(across(c(district, province), as.factor),
            version = as.integer(version),
            internal_id = as.integer(internal_id),
            district = na_if(district, "XXX"),
            province = na_if(province, "XX"),
            country = "Italy"
        )
}

describe.brunetti.region_simple_ <- function(data) {
    data |>
        separate_wider_regex(identifier, c("TM[XN]D_", anagrafica = "[:upper:]*", user_code = "(?:[:digit:]{6})?", MG = "(?:_MG)?", "(?:\\.[:lower:]+)?"), cols_remove = FALSE) |>
        mutate(MG = str_detect(MG, "_MG"))
}

describe.brunetti.region_at_ <- function(data) {
    data |>
        separate_wider_regex(identifier, c("TM[XN]D_", country = "[:alpha:]{2}", "_", anagrafica = "[[:upper:]_]+?", GSOD = "(?:_GSOD)?", "_", user_code = "[:digit:]{6}", MG = "(?:_MG)?", "(?:\\.[:lower:]+)?"), cols_remove = FALSE) |>
        mutate(GSOD = str_detect(GSOD, "_GSOD"), MG = str_detect(MG, "_MG"))
}

describe.brunetti.region_ita_ <- function(data) {
    data |>
        separate_wider_regex(identifier, c("TM[XN]D_", country = "[:alpha:]{2,3}", "_", district = "[:alpha:]{3}", "_", province = "[:alpha:]{2}", "_", anagrafica = "[[:upper:]_[:digit:]-]*?", "_?", user_code = "t[:digit:]{4}|(?:S|N)?[:digit:]{3,4}|[:digit:]{5}[:upper:]{2}|", MG = "(?:_MG)?", "(?:\\.[:lower:]+$)?"), cols_remove = FALSE) |>
        mutate(
            MG = str_detect(MG, "_MG"),
            district = na_if(district, "XXX"),
            province = na_if(province, "XX")
        )
}

italian_districts_ <- tibble(
    code = c("ABR", "BAS", "CAL", "CAM", "EMR", "FVG", "LAZ", "LIG", "LOM", "MAR", "MOL", "PIE", "PUG", "SAR", "SIC", "TOS", "TAA", "UMB", "VDA", "VEN"),
    name = c("Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", "Friuli-Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche", "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana", "Trentino-Alto Adige", "Umbria", "Valle D'aosta", "Veneto")
)

widths.series_ <- c(5, 3, rep(7, 31))
read.brunetti.series <- function(db, tvar, flavor, identifiers = NULL) {
    if (is.null(identifiers)) {
        path_s <- path.datafile(db, tvar, flavor, check = TRUE)
    } else {
        path_s <- file.path(path.root(db, tvar, flavor), brunetti.series_file.relative(flavor, identifiers))
    }
    vroom_fwf(path_s,
        fwf_widths(widths.series_, col_names = c("year", "month", seq.int(1, 31))),
        col_types = cols(year = "i", month = "i", .default = "d"),
        na = c("", "NA", "-90.0"), id = "file", progress = FALSE, num_threads = 7
    ) |>
        mutate(identifier = factor(str_split_i(file, "/", -1) |> str_split_i(fixed("."), 1)), .keep = "unused", .before = 1) |>
        pivot_longer(cols = seq(4, 34), names_to = "day", values_to = tvar, names_transform = as.integer) |>
        mutate(date = make_date(year, month, day), .keep = "unused") |>
        filter(!is.na(date))
}

read.brunetti.series.single <- function(db, tvar, flavor, identifier) {
    vroom_fwf(file.path(path.root(db, tvar, flavor), brunetti.series_file.relative(flavor, identifier)),
        fwf_widths(widths.series_, col_names = c("year", "month", seq.int(1, 31))),
        col_types = cols(year = "i", month = "i", .default = "d"),
        na = c("", "NA", "-90.0"), progress = FALSE
    ) |>
        pivot_longer(cols = seq(3, 33), names_to = "day", values_to = tvar, names_transform = as.integer) |>
        mutate(date = make_date(year, month, day), .keep = "unused") |>
        drop_na(date)
}
