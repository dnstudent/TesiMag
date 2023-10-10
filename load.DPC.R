source("paths.R")

library(dplyr)
library(stringr)
library(tidyr)
library(tidyselect)
library(vroom)

end.region_full <- function(var) {
    if (var == "T_MAX") {
        return(3737)
    }
}

end.region2 <- function(var) {
    if (var == "T_MAX") {
        return(3866)
    }
}

end.region3 <- function(var) {
    if (var == "T_MAX") {
        return(3868)
    }
}

load.DPC.region_fwf <- function(fpath, first, last = NULL) {
    if (is.null(last)) {
        last <- first
        first <- 1
    }
    widths.region_full <- c(86, 9, 12, 7, 9)
    vroom_fwf(fpath, fwf_widths(widths.region_full, col_names = c("station", "lon", "lat", "elevation", "internal_id")), col_types = cols(station = "c", internal_id = "i", .default = "d"), skip = first - 1, n_max = last - first + 1)
}

mutate.DPC.region_fwf <- function(data, ...) {
    data |>
        separate_wider_regex(station, c("T[XN]_", state = "[:alpha:]{3}", "_", province = "[:alpha:]{2}", "_", loc = "[[:alnum:]_]+", "_", digits = "[:digit:]{2}", "_", "[:digit:]{9}"), cols_remove = FALSE, ...) |>
        mutate(across(c(state, province), as.factor),
            digits = as.integer(digits),
            state = na_if(state, "XXX"),
            province = na_if(province, "XX")
        )
}

load.DPC.region_ssv <- function(fpath, first, last = Inf) {
    vroom(
        fpath,
        delim = " ",
        col_names = c("station", "lon", "lat", "elevation"),
        col_types = cols(station = "c", .default = "d"),
        skip = first - 1, n_max = last - first + 1
    )
}

mutate.DPC.simple <- function(data, ...) {
    data |>
        separate_wider_regex(station, c("TM[XN]D_", loc = "[:upper:]*", user_code = "(?:[:digit:]{6})?", MG = "(?:_MG)?"), cols_remove = FALSE, ...) |>
        mutate(MG = str_detect(MG, "_MG"))
}

mutate.DPC.region_at <- function(data, ...) {
    data |>
        separate_wider_regex(station, c("TM[XN]D_", nation = "[:alpha:]{2}", "_", loc = "[[:upper:]_]+?", GSOD = "(?:_GSOD)?", "_", user_code = "[:digit:]{6}", MG = "(?:_MG)?"), cols_remove = FALSE, ...) |>
        mutate(GSOD = str_detect(GSOD, "_GSOD"), MG = str_detect(MG, "_MG"))
}

mutate.DPC.region_ita <- function(data, ...) {
    data |>
        separate_wider_regex(station, c("TM[XN]D_", nation = "[:alpha:]{2,3}", "_", state = "[:alpha:]{3}", "_", province = "[:alpha:]{2}", "_", loc = "[[:upper:]_[:digit:]-]*?", "_?", user_code = "t[:digit:]{4}|(?:S|N)?[:digit:]{3,4}|[:digit:]{5}[:upper:]{2}|", MG = "(?:_MG)?$"), cols_remove = FALSE, ...) |>
        mutate(
            MG = str_detect(MG, "_MG"),
            state = na_if(state, "XXX"),
            province = na_if(province, "XX")
        )
}

load.DPC.stations <- function(tvar, ...) {
    fpath <- path.stations("DPC", tvar)
    data <- bind_rows(
        load.DPC.region_fwf(fpath, 3737),
        load.DPC.region_ssv(fpath, 3738)
    )
    if (tvar == "T_MIN") {
        data <- bind_rows(
            data |> slice(1:3737) |> mutate.DPC.region_fwf(...),
            data |> slice(3738:3868, 3897:3902, 4119:4134) |> mutate.DPC.simple(...),
            data |> slice(3869:3896) |> mutate.DPC.region_at(...),
            data |> slice(3903:4118) |> mutate.DPC.region_ita(...)
        )
    } else if (tvar == "T_MAX") {
        data <- bind_rows(
            data |> slice(1:3737) |> mutate.DPC.region_fwf(...),
            data |> slice(3738:3868, 3897:3902, 4126:4141) |> mutate.DPC.simple(...),
            data |> slice(3869:3896) |> mutate.DPC.region_at(...),
            data |> slice(3903:4125) |> mutate.DPC.region_ita(...)
        )
    } else {
        simpleError("Invalid tvar")
    }
    data |> mutate(
        GSOD = replace_na(GSOD, FALSE),
        MG = replace_na(MG, FALSE),
        across(where(is.character), \(x) na_if(x, "")),
        nation = as.factor(nation),
        state = as.factor(state),
        province = as.factor(province)
    )
}
