setwd(fs::path_abs("~/Local_Workspace/TesiMag"))
Sys.setlocale("LC_ALL", "UTF-8")
source("src/database/startup.R")
source("src/database/query/data.R")
library(openxlsx, warn.conflicts = FALSE)

conns <- load_dbs()
query_checkpoint_meta("full", "merged_corrected", conns$data) |>
    collect() |>
    rowwise() |>
    mutate(across(where(is.list), ~ paste0(., collapse = ";"))) |>
    ungroup() |>
    write_csv_arrow(fs::path("db", "conv", "merged_corrected", "metadata.csv"), write_options = csv_write_options(quoting_style = "AllValid"))

open_dataset(fs::path("db", "extra", "merge_specs")) |>
    collect() |>
    rename(from_dataset = dataset, from_sensor_key = sensor_key, dataset = set, sensor_key = gkey) |>
    write_csv_arrow(fs::path("db", "conv", "merged_corrected", "merge_specs.csv"), write_options = csv_write_options(quoting_style = "AllValid"))
