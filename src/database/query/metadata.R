library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(DBI, warn.conflicts = FALSE)

source("src/database/query/spatial.R")

similar_namesdist <- function(metadata, statconn, threshold = 0.8) {
    metadata <- select(metadata, name, key, lon, lat)
    pairs <- cross_join(metadata, metadata, suffix = c("_x", "_y")) |>
        filter(key_x != key_y) |>
        mutate(
            norm_name_x = name_x |> lower() |> replace("_", " ") |> nfc_normalize() |> strip_accents() |> trim(),
            norm_name_y = name_y |> lower() |> replace("_", " ") |> nfc_normalize() |> strip_accents() |> trim(),
            strSym = jaro_winkler_similarity(norm_name_y, norm_name_x)
        ) |>
        filter(strSym >= threshold) |>
        collect()
    query_distance(pairs, statconn) |>
        collect() |>
        left_join(pairs |> select(key_x, key_y, strSym), by = c("key_x", "key_y"))
}
