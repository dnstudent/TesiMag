library(vroom)
library(stringr)
library(tidyr)

read_merge_specs <- function(root_path) {
  cat("Reading ", fs::path(root_path, "metadata", "merge_specs.csv"), "\n")
  fs::path(root_path, "metadata", "merge_specs.csv") |>
    vroom(
      col_types = cols(
        dataset = col_character(),
        series_key = col_integer(),
        from = col_character(),
        variable = col_integer(),
        metadata_rank = col_integer(),
        data_rank = col_integer(),
        offset = col_integer(),
        force_zero_correction = col_logical(),
        force_merge = col_logical(),
        merged = col_logical(),
        would_integrate = col_integer(),
        n_joint_days = col_integer(),
        .default = col_guess()
      )
    )
}

read_metadata <- function(root_path) {
  cat("Reading ", fs::path(root_path, "metadata", "metadata.csv"), "\n")
  fs::path(root_path, "metadata", "metadata.csv") |>
    vroom(
      col_types = cols(
        dataset = col_character(),
        series_key = col_integer(),
        user_code = col_character(),
        from = col_character(),
        lon = col_double(),
        lat = col_double(),
        elevation = col_double(),
        elevation_glo30 = col_double(),
        valid_days = col_integer(),
        valid90 = col_integer(),
        keep = col_logical(),
        manual_loc_correction = col_logical(),
        manual_elev_correction = col_logical(),
        .default = col_character()
      )
    )
}

read_data_tables <- function(root_path, dataset, series_key) {
  paths <- fs::path(root_path, "data", str_c(c("TMND", "TMXD"), dataset, str_pad(series_key, 3L, side = "left", pad = "0"), sep = "_"), ext = "csv")
  cat("Reading ", paths, "\n")
  vroom(
    paths,
    col_types = cols(
      date = col_date(format = "%Y-%m-%d"),
      master = col_double(),
      from = col_character(),
      .default = col_double()
    ),
    id = "file"
  ) |>
    mutate(
      # file = fs::path_file(file) |> fs::path_ext_remove(),
      dataset = !!dataset,
      series_key = !!series_key,
      variable = str_sub(fs::path_file(file) |> fs::path_ext_remove(), 1L, 4L) |> case_match("TMND" ~ -1L, "TMXD" ~ 1L)
    )
}
