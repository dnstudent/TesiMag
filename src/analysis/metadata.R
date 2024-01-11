library(dplyr, warn.conflicts = FALSE)

metadata_analysis <- function(series_matches, metadata) {
    metadata <- metadata |> select(
        dataset, id, name, network, state, first_registration, last_registration, elevation, glo30m_elevation, glo30asec_elevation
    )
    series_matches |>
        left_join(metadata, join_by(id_x == id)) |>
        left_join(metadata, join_by(id_y == id), suffix = c("_x", "_y")) |>
        mutate(
            delH = elevation_y - elevation_x,
            delZm = glo30m_elevation_y - glo30m_elevation_x,
            delZsec = glo30asec_elevation_y - glo30asec_elevation_x,
            norm_name_x = name_x |> lower() |> replace("_", " ") |> nfc_normalize() |> strip_accents() |> trim(),
            norm_name_y = name_y |> lower() |> replace("_", " ") |> nfc_normalize() |> strip_accents() |> trim(),
            strSym = jaro_winkler_similarity(norm_name_y, norm_name_x),
        )
}
