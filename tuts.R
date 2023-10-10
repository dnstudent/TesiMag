library(spData)
library(sf)

world %>%
  group_by(continent) %>%
  st_centroid()