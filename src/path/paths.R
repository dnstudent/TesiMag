source("paths.ITA.R")
library(fs)
library(stringr)

path.ds <- "/Users/davidenicoli/Local_Workspace/Datasets"

brun_t <- function(tvar) {
  switch(tvar,
    T_MAX = "TX",
    T_MIN = "TN",
    stop("Invalid variable name: ", tvar)
  )
}

section.SCIA <- function(tvar) {
  file.path("SCIA", "giornaliere", switch(tvar,
    T_MAX = "massime",
    T_MIN = "minime",
    stop("Invalid variable name: ", tvar)
  ))
}

section.BRUN <- function(tvar, which_one) {
  tvar <- brun_t(tvar)
  file.path(
    "DPC",
    switch(which_one,
      raw = "01_DATI_SINTESI_",
      qc_era5 = "03_STEP02_TH05_QCSYNT_DATA_DD_DPC_",
      qc_homo = "04_QC_DATA_DD_DPC_",
      stop("Invalid section name: ", which_one)
    ) |> paste0(tvar)
  )
}

section.DPC <- section.BRUN

path.section <- function(db, tvar, ...) {
  file.path(path.ds, do.call(paste("section", db, sep = "."), list(tvar, ...)))
}

name.brun.anagrafica <- function(db, which_one) {
  switch(which_one,
    raw = switch(db,
      DPC = "ANAGRAFICA",
      BRUN = "ANAGRAFICA_TOT",
      stop("Invalid database name: ", db)
    ),
    qc_era5 = "ANAGRAFICA",
    qc_homo = "ANAGRAFICA_OK"
  )
}

path.anagrafica <- function(db, tvar, ...) {
  rest <- switch(db,
    SCIA = file.path("db", "wfs_stations.parquet"),
    DPC = name.brun.anagrafica("DPC", ...),
    BRUN = name.brun.anagrafica("BRUN", ...),
    stop("Invalid database name: ", db)
  )
  file.path(path.section(db, tvar, ...), rest)
}

path.SCIA.stations.corrections <- function(tvar) {
  file.path(path.section("SCIA", tvar), "db", "station_interop.parquet")
}

ls.BRUN <- function(tvar, which_one) {
  file.path(
    path.section("BRUN", tvar, which_one),
    path.anagrafica("BRUN", tvar, which_one) |>
      readLines() |>
      str_split_i(" ", 1)
  )
}

ls.DPC <- function(tvar, which_one) {
  names <- path.anagrafica("DPC", tvar, which_one) |>
    readLines() |>
    str_match("^T[XN]_[:alpha:]{3}_[:alpha:]{2}_[[:alnum:]_]+_[:digit:]{2}_[:digit:]{9}\\.?[:alpha:]*")
  file.path(path.section("DPC", tvar, which_one), names[!is.na(names)])
}

ls.SCIA <- function(tvar) {
  file.path(path.section("SCIA", tvar), "db", "series.parquet")
}

path.series <- function(db, tvar, ..., check = FALSE) {
  files <- do.call(paste("ls", db, sep = "."), list(tvar, ...))
  if (check) {
    assertthat::assert_that(
      file.exists(files) |> all(),
      msg = "Some files do not exist"
    )
  }
  files
}
