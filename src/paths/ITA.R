path.ds <- "/Users/davidenicoli/Local_Workspace/Datasets"

path.COP30 <- file.path(path.ds, "COPERNICUS DEM30")
path.cached.COP30 <- fs::path_rel("./temp/dem/dem.tif")
path.boundaries.italy.states <- file.path(path.ds, "geoBoundaries", "ITA-ADM1", "geoBoundaries-ITA-ADM1.geojson")

ls.COP30.missing <- function() {
    list.files(path = file.path(path.COP30, "missing"), recursive = FALSE, full.names = TRUE, pattern = "^missing_N\\d{2}_E0\\d{2}.tif$")
}


ls.COP30.missing.WBM <- function() {
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
        ls.COP30.missing.WBM()
    )
}

ls.COP30.HEM <- function() {
    c(
        list.files(path = path.COP30, recursive = TRUE, full.names = TRUE, pattern = "^Copernicus_DSM_10_N\\d{2}_00_E0\\d{2}_00_HEM.tif$"),
        ls.COP30.missing.WBM()
    )
}
