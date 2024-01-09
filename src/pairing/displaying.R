library(dplyr, warn.conflicts = FALSE)
library(openxlsx, warn.conflicts = FALSE)

source("src/load/tools.R")

meaningful_columns <- function(table, ...) {
    select(table, variable, starts_with("identifier"), starts_with("anagrafica"), distance, f0, starts_with("del"), ends_with("T"), minilap, valid_days_union, valid_days_inters, ...)
}
ana <- function(table, ...) {
    select(table, starts_with("anagr"), starts_with("identif"), ...)
}
clean_from <- function(analysis_table, matched) {
    analysis_table |>
        anti_join(matched, by = c("variable", "series_id.x")) |>
        anti_join(matched, by = c("variable", "series_id.y"))
}
write_xslx_analysis <- function(analysis, to, ...) {
    analysis <- analysis |> select(
        starts_with("id"),
        variable,
        offset_days,
        starts_with("dataset"),
        starts_with("name"),
        starts_with("network"),
        strSym,
        H = elevation_x,
        delH,
        delZm,
        delZsec,
        distance,
        f0,
        fsameint,
        delT,
        maeT,
        monthlydelT,
        monthlymaeT,
        climaticdelT,
        climaticmaeT,
        valid_days_union,
        valid_days_inters,
        valid_days_x,
        valid_days_y,
        qc_clim_available, ...
    )
    class(analysis$strSym) <- "percentage"
    class(analysis$f0) <- "percentage"
    class(analysis$fsameint) <- "percentage"
    wb <- createWorkbook()
    addWorksheet(wb, "data")
    writeDataTable(wb, 1, analysis)

    integer_style <- createStyle(numFmt = "0")
    addStyle(wb, 1, integer_style, rows = 1:5000, cols = 12:16, gridExpand = TRUE)

    prec2_style <- createStyle(numFmt = "0.00")
    addStyle(wb, 1, prec2_style, rows = 1:5000, cols = 19:24, gridExpand = TRUE)

    outTStyle <- createStyle(fontColour = "#9C0006", bgFill = "#FFC7CE")
    conditionalFormatting(wb, 1, cols = c(19, 24), rows = 1:5000, rule = "<-0.5", style = outTStyle)
    conditionalFormatting(wb, 1, cols = c(19, 24), rows = 1:5000, rule = ">0.5", style = outTStyle)

    saveWorkbook(wb, to, overwrite = TRUE)
}

#' Launch a leaflet map showing the stations in the given database.
#' Stations are identified by colored markers, each color corresponding to a different dataset.
launch_leaflet <- function(database) {
    map <- leaflet::leaflet() |>
        leaflet::addTiles()

    stations <- database$meta |>
        collect() |>
        st_md_to_sf()

    cs <- c("red", "blue", "green", "purple", "orange", "cadetblue", "beige", "darkgreen", "lightgreen", "darkblue", "lightblue", "darkpurple", "pink", "white", "gray", "lightgray", "black")

    dataset_colors <- stations |>
        distinct(dataset_id)
    dataset_colors <- dataset_colors |>
        mutate(dataset_color = head(cs, nrow(dataset_colors)))

    groups <- stations |>
        left_join(dataset_colors, by = "dataset_id") |>
        group_by(dataset_id, dataset_color) |>
        group_split()

    for (group in groups) {
        map <- map |> leaflet::addAwesomeMarkers(
            data = group,
            label = ~station_name,
            popup = ~ stringr::str_glue("{station_name}, {elevation} m, {network}"),
            icon = leaflet::awesomeIcons(
                markerColor = ~dataset_color,
            )
        )
    }
    map |> leaflet.extras::addSearchOSM()
}
