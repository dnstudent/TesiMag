library(dplyr)
library(ggplot2)
library(ggrepel)
library(lubridate)
library(stringr)
library(arrow)
library(sf)
library(tsibble)
library(withr)
library(tikzDevice)
library(extrafont)
library(kableExtra)
source("src/database/startup.R")
source("src/database/query/data.R")
source("scripts/common.R")

loadfonts()
theme_set(theme_bw() + theme_defaults)
options(tikzDefaultEngine = "xetex")

dotenv::load_dot_env(fs::path("scripts", ".env"))
pres_dir <- fs::path(Sys.getenv("PRES_DIR"), "introduzione")
if (!fs::dir_exists(pres_dir)) {
    fs::dir_create(pres_dir)
}
conns <- load_dbs()
on.exit(close_dbs(conns))

meta <- query_checkpoint_meta("SCIA", "raw", conns$data) |>
    collect() |>
    st_as_sf(coords = c("lon", "lat"), crs = "EPSG:4326", remove = FALSE)
# data <- query_checkpoint_data("SCIA", "raw", conns$data)
ggplot() +
    geom_sf(data = meta)
