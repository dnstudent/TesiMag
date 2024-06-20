library(dplyr, warn.conflicts = FALSE)
library(openxlsx, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)

source("src/load/tools.R")

show_merge <- function(merged_meta, unmerged_meta, ...) {
  unmerged_meta <- collect(unmerged_meta)
  colnms <- paste0(rep(c(...), each = 2L), rep(c("_master", "_merged"), length(c(...))))
  merged_meta |>
    collect() |>
    rowwise() |>
    mutate(master_key = first(from_sensor_keys), master_ds = first(from_datasets)) |>
    ungroup() |>
    select(from_sensor_keys, from_datasets, master_key, master_ds) |>
    unnest_longer(col = c(from_sensor_keys, from_datasets)) |>
    left_join(unmerged_meta, by = c("master_key" = "sensor_key", "master_ds" = "dataset")) |>
    left_join(unmerged_meta, by = c("from_sensor_keys" = "sensor_key", "from_datasets" = "dataset"), suffix = c("_master", "_merged")) |>
    select(
      master_key,
      master_ds,
      from_sensor_keys,
      from_datasets,
      name_master,
      name_merged,
      any_of(colnms)
    )
}

write_xlsx_analysis <- function(analysis, to, ..., .format = TRUE) {
  analysis <- analysis |> select(
      starts_with("sensor_key"),
      starts_with("dataset"),
      starts_with("network"),
      starts_with("name"),
      any_of(c(
      "variable",
      "offset_days", # 10
      "strSym",
      "elevation_x",
      "delH",
      "distance",
      "f0",
      "f0noint",
      "fsameint",
      "balance",
      "delT",
      "maeT",
      "monthlydelT",
      "monthlymaeT",
      "climaticdelT",
      "climaticmaeT",
      "valid_days_union",
      "valid_days_inters",
      "valid_days_x",
      "valid_days_y",
      "overlap_x",
      "overlap_y",
      "sensor_first_x",
      "sensor_first_y",
      "common_period", # 32
      "common_period_x",
      "common_period_y" # 33,34
    )),
    ...
  )

  if ("tag_same_series" %in% colnames(analysis)) {
    analysis <- analysis |> relocate(tag_same_series)
    off <- 1L
  } else {
    off <- 0L
  }
    class(analysis$strSym) <- "percentage"
    class(analysis$f0) <- "percentage"
    class(analysis$f0noint) <- "percentage"
    class(analysis$fsameint) <- "percentage"
    options("openxlsx.numFmt" = "#.##")
  if (.format) {
    class(analysis$overlap_min) <- "percentage"
    class(analysis$overlap_max) <- "percentage"
    class(analysis$overlap_union) <- "percentage"
    class(analysis$common_period_vs_x) <- "percentage"
    class(analysis$common_period_vs_y) <- "percentage"
  }
  wb <- createWorkbook()
  addWorksheet(wb, "data")
  writeDataTable(wb, 1, analysis, tableStyle = "TableStyleMedium2")
  if (.format) {
    integer_style <- createStyle(numFmt = "0")
    addStyle(wb, 1, integer_style, rows = 1:nrow(analysis), cols = (off + 12):(off + 14), gridExpand = TRUE)

    prec2_style <- createStyle(numFmt = "0.00")
    addStyle(wb, 1, prec2_style, rows = 1:nrow(analysis), cols = (off + 18):(off + 24), gridExpand = TRUE)

    outTStyle <- createStyle(fontColour = "#9C0006", bgFill = "#FFC7CE")
    conditionalFormatting(wb, 1, cols = c(off + 19, off + 24), rows = 1:nrow(analysis), rule = "<-0.5", style = outTStyle)
    conditionalFormatting(wb, 1, cols = c(off + 19, off + 24), rows = 1:nrow(analysis), rule = ">0.5", style = outTStyle)
  }
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

leaflet_groups <- function(grouped_stations, metadata, gid_variable, extra_display = c()) {
  map <- leaflet::leaflet() |>
    leaflet::addTiles()

  stations <- metadata |>
    slice_sample(n = nrow(metadata)) |>
    right_join(grouped_stations, by = c("dataset", "sensor_key")) |>
    st_md_to_sf()


  group_colors <- stations |>
    distinct({{ gid_variable }})

  cs <- rainbow(nrow(group_colors)) # c("red", "blue", "green", "purple", "orange", "cadetblue", "beige", "darkgreen", "lightgreen", "darkblue", "lightblue", "darkpurple", "pink", "white", "gray", "lightgray", "black")

  group_colors <- group_colors |>
    mutate(group_color = cs) # rep_len(cs, nrow(group_colors)))

  groups <- stations |>
    left_join(group_colors, by = join_by({{ gid_variable }})) |>
    group_by({{ gid_variable }}, group_color) |>
    group_split()



  for (group in groups) {
    map <- map |> leaflet::addAwesomeMarkers(
      data = group,
      label = ~name,
      popup = ~ stringr::str_glue("{name}, {elevation} m, {network}"),
      icon = leaflet::awesomeIcons(
        iconColor = ~group_color,
        markerColor = "white"
      )
    )
  }
  map |> leaflet.extras::addSearchOSM()
}


render_map <- function(map) {
  library(shiny, warn.conflicts = FALSE)
  library(leaflet, warn.conflicts = FALSE)

  ui <- fluidPage(
    leafletOutput("mymap"),
    p()
  )

  server <- function(input, output, session) {
    output$mymap <- renderLeaflet(map)
  }

  shinyApp(ui, server)
}
