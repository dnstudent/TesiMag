library(stars)
library(ggplot2)
library(tidyr)
library(padr)

source("load.R")

raster_files <- ls.COP30()
full_dem <- raster_files |>
  st_mosaic() |>
  read_stars()
bbox <- st_bbox(c(xmin = 6.9, xmax = 8.1, ymin = 44.8, ymax = 46.4), crs = st_crs(full_dem))

massime_series <- load.SCIA.T_MAX() %>%
  filter(date >= as.Date("2010-01-01")) %>%
  pivot_wider(names_from = internal_id, values_from = `Temperatura massima `) %>%
  arrange(date) %>%
  pad()

station_meta <- load.SCIA.stations()
