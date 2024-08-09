library(vroom, warn.conflicts = F)
library(arrow, warn.conflicts = F)
library(lubridate, warn.conflicts = F)

source("src/paths/paths.R")
source("notebooks/ds_nazionali/SWISS/procedures/load_daily.R")

dataset_spec <- function() {
    list(
        "https://opendata.swiss/it/dataset/klimamessnetz-tageswerte",
        "national",
        "Dataset MeteoSwiss NBCN: rete climatologica di base nazionale. 29 stazioni con serie a lungo termine."
    )
}

load_meta <- function(...) {
    load_general_meta(...) |> mutate(dataset = "MeteoSwissNBCN")
}


load_data <- function() {
    data_dir <- fs::path(path.ds, "MeteoSwiss", "NBCN-daily", "fragments")
    open_delim_dataset(data_dir,
        schema = schema(
            `station/location` = utf8(),
            date = int32(),
            gre000d0 = utf8(),
            hto000d0 = utf8(),
            nto000d0 = utf8(),
            prestad0 = utf8(),
            rre150d0 = utf8(),
            sre000d0 = utf8(),
            tre200d0 = utf8(),
            tre200dn = float64(),
            tre200dx = float64(),
            ure200d0 = utf8()
        ),
        na = c("-"),
        delim = ";",
        skip = 1L
    ) |>
        select(series_id = `station/location`, date, T_MIN = tre200dn, T_MAX = tre200dx) |>
        mutate(date = lubridate::ymd(date)) |>
        to_duckdb() |>
        tidyr::pivot_longer(cols = c(T_MIN, T_MAX), names_to = "variable", values_to = "value") |>
        to_arrow() |>
        filter(!is.na(value)) |>
        mutate(dataset = "MeteoSwissNBCN") |>
        compute()
}

load_daily_data.meteoswiss_nbcn <- function() {
    list(meta = load_meta(), data = load_data())
}
