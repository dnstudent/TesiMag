source("src/paths/ITA.R")
source("src/paths/DPC.R")
source("src/paths/BRUN.R")
source("src/paths/SCIA.R")
library(fs)
library(stringr)

path.ds <- "/Users/davidenicoli/Local_Workspace/Datasets"

path.root <- function(db, tvar, ...) {
  file.path(
    path.ds,
    do.call(paste(db, "root", "relative", sep = "."), list(tvar, ...))
  )
}

path.metadata <- function(db, tvar, ...) {
  file.path(
    path.root(db, tvar, ...),
    do.call(paste(db, "anagrafica_file.relative", sep = "."), list(tvar, ...))
  )
}

ls.BRUN.datafiles <- function(tvar, flavor) {
  file.path(
    path.root("BRUN", tvar, flavor),
    path.metadata("BRUN", tvar, flavor) |>
      readLines() |>
      str_split_i(" ", 1)
  )
}

ls.DPC.datafiles <- function(tvar, flavor) {
  names <- path.metadata("DPC", tvar, flavor) |>
    readLines() |>
    str_match("^T[XN]_[:alpha:]{3}_[:alpha:]{2}_[[:alnum:]_]+_[:digit:]{2}_[:digit:]{9}\\.?[:lower:]*")
  file.path(
    path.root("DPC", tvar, flavor),
    names[!is.na(names)]
  )
}

ls.SCIA.datafiles <- function(tvar) {
  file.path(path.root("SCIA", tvar), "db", "series.parquet")
}

path.datafile <- function(db, tvar, ..., check = FALSE) {
  files <- do.call(paste("ls", db, "datafiles", sep = "."), list(tvar, ...))
  if (check) {
    existence <- file.exists(files)
    assertthat::assert_that(
      all(existence),
      msg = paste0("Some files do not exist: ", files[!existence])
    )
  }
  files
}
