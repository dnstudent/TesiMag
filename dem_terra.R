library(terra)

source("vars.R")

raster_files <- ls.COP30()
full_dem <- vrt(raster_files)
plot(full_dem)
