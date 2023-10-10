library(fs)

path.ds <- "/Users/davidenicoli/Local_Workspace/Datasets"
path.COP30 <- file.path(path.ds, "COPERNICUS DEM30")
name.DPC.ex <- "TMND_IT_PIE_CN_PONTECHIANALE_283_MG"
path.boundaries.italy.states <- file.path(path.ds, "geoBoundaries", "ITA-ADM1", "geoBoundaries-ITA-ADM1.topojson")

ls.COP30.missing <- function() {
  list.files(path = file.path(path.COP30, "missing"), recursive = FALSE, full.names = TRUE, pattern = "^missing_N\\d{2}_E0\\d{2}_WBM.tif$")
}

ls.COP30 <- function() {
  c(
    list.files(path = path.COP30, recursive = TRUE, full.names = TRUE, pattern = "^Copernicus_DSM_10_N\\d{2}_00_E0\\d{2}_00_DEM.tif$"),
    ls.COP30.missing()
  )
}

ls.COP30.WBM <- function() {
  c(
    list.files(path = path.COP30, recursive = TRUE, full.names = TRUE, pattern = "^Copernicus_DSM_10_N\\d{2}_00_E0\\d{2}_00_WBM.tif$"),
    ls.COP30.missing()
  )
}

ls.COP30.HEM <- function() {
  c(
    list.files(path = path.COP30, recursive = TRUE, full.names = TRUE, pattern = "^Copernicus_DSM_10_N\\d{2}_00_E0\\d{2}_00_HEM.tif$"),
    ls.COP30.missing()
  )
}

path.section <- function(network, var) {
  if (network == "SCIA") {
    if (var == "T_MAX") {
      var <- "massime"
    } else if (var == "T_MIN") {
      var <- "minime"
    } else {
      stop("Invalid variable name")
    }
    var <- file.path("giornaliere", var)
  } else if (network == "DPC") {
    if (var == "T_MAX") {
      var <- "01_DATI_SINTESI_TX"
    } else if (var == "T_MIN") {
      var <- "01_DATI_SINTESI_TN"
    } else {
      stop("Invalid variable name")
    }
  } else {
    stop("Invalid network name")
  }
  file.path(path.ds, network, var)
}

path.stations <- function(network, var) {
  rest <- NULL
  if (network == "SCIA") {
    rest <- file.path("db", "wfs_stations.parquet")
  } else if (network == "DPC") {
    rest <- "ANAGRAFICA_TOT"
  } else {
    stop("Invalid network name")
  }
  file.path(path.section(network, var), rest)
}

path.SCIA.stations.corrections <- function(var) {
  file.path(path.section("SCIA", var), "db", "station_interop.parquet")
}

ls.DPC <- function(var) {
  fs::dir_ls(path = path.section("DPC", var), regexp = "LOG|ANAGRAFICA.*|DOPPIONI|*.gre", invert = TRUE)
}

path.series <- function(network, var) {
  rest <- NULL
  if (network == "SCIA") {
    return(file.path(path.section(network, var), "db", "series.parquet"))
  } else if (network == "DPC") {
    return(ls.DPC(var))
  } else {
    stop("Invalid network name")
  }
}
