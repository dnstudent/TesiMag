source("src/paths/paths.R")

load.DEM.COP30 <- function(cached = TRUE) {
    if (cached) {
        rast(path.cached.COP30)
    } else {
        ls.COP30() |>
            terra::sprc() |>
            terra::merge()
    }
}

missing_coords <- list(
    c(5, 37),
    c(5, 38),
    c(5, 39),
    c(5, 40),
    c(5, 41),
    c(5, 42),
    c(6, 38),
    c(6, 39),
    c(6, 40),
    c(6, 41),
    c(7, 38),
    c(7, 39),
    c(7, 40),
    c(7, 41),
    c(7, 42),
    c(10, 38),
    c(10, 39),
    c(10, 40),
    c(10, 41),
    c(11, 38),
    c(11, 39),
    c(11, 40),
    c(12, 39),
    c(13, 39),
    c(13, 35),
    c(13, 36),
    c(14, 39),
    c(14, 43),
    c(18, 41),
    c(15, 35),
    c(16, 35),
    c(16, 36),
    c(17, 35),
    c(17, 36),
    c(17, 37),
    c(18, 35),
    c(18, 36),
    c(18, 37),
    c(18, 38),
    c(19, 35),
    c(19, 36),
    c(19, 37),
    c(19, 38),
    c(20, 35),
    c(20, 36)
)

build.DEM.COP30.sea_mask <- function() {
    files <- c(ls.COP30.WBM(), ls.COP30.WBM.missing())
    wbm_data <- vrt(files)
    sea <- wbm_data == 1
    sea[!sea] <- NA
    sea
}

load.italian_boundaries <- function(level, ...) {
    sf::st_read(path.boundaries.italy(level), quiet = TRUE, ...)
}
