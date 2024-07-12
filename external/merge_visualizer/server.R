library(ggplot2)
library(plotly)
library(vroom)
library(dplyr)
library(stringr)
library(zeallot)

source("R/read.R")
source("R/utils.R")

root_path <- fs::path_expand("../../db/conv/merged_corrected")
merge_specs <- read_merge_specs(root_path) |>
  relocate(merged) |>
  unite(master_tag, dataset, series_key, sep = "/", remove = FALSE) |>
  arrange(data_rank)
# metadata <- read_metadata(root_path)
tconv <- tibble(variable = c(-1L, 1L), variable_name = factor(c("TMND", "TMXD")))

plot_diffs <- function(df) {
  df |>
    plot_ly(x = ~date, split = ~variable_name) |>
    add_trace(y = ~diff, type = "scatter", mode = "markers", color = ~variable_name, colors = c("TMND" = "blue", "TMXD" = "red")) |>
    add_trace(y = ~correction, type = "scatter", mode = "lines", color = I("gray")) |>
    layout(yaxis = list(title = "Correction"), xaxis = list(title = "Date"))
}

master_specs <- function(master_tag) {
  pieces <- str_split_fixed(master_tag, "/", 2L)
  list(dataset = pieces[1L, 1L], series_key = as.integer(pieces[1L, 2L]))
}

server <- function(input, output, session) {
  build_master_list <- observe(
    {
      cat("Building master list\n")
      updateSelectizeInput(inputId = "masterSelect", choices = unique(merge_specs$master_tag), server = TRUE)
    },
    priority = 99L
  )

  integ_series <- reactive({
    cat(str_glue("Recomputing integ_series with master '{input$masterSelect}'"), "\n")
    master_pieces <- master_specs(input$masterSelect)
    master_pieces <- tibble(dataset = master_pieces[[1L]], series_key = master_pieces[[2L]])
    merge_specs |>
      semi_join(master_pieces, by = c("dataset", "series_key"))
  }) |> bindEvent(input$masterSelect, ignoreInit = TRUE, ignoreNULL = FALSE)

  observe({
    cat("Updating integratorSelect\n")
    updateSelectInput(inputId = "integratorSelect", choices = unique(integ_series()$from))
  }) |> bindEvent(integ_series())

  merge_data <- reactive({
    cat(str_glue("Recomputing merge_data with master '{input$masterSelect}'"), "\n")
    c(master_ds, master_key) %<-% master_specs(input$masterSelect)
    read_data_tables(root_path, master_ds, master_key) |>
      left_join(tconv, by = "variable")
  }) |> bindEvent(input$masterSelect, ignoreInit = TRUE, ignoreNULL = FALSE)

  integ_data <- reactive({
    cat("Recomputing integ_data\n")

    data_ranks <- integ_series() |> select(from, variable, data_rank)
    integrator_infos <- integ_series() |>
      filter(from == input$integratorSelect) |>
      select(variable, k0, a1, b1, a2, b2, integrator_data_rank = data_rank)

    m <- merge_data() |>
      left_join(data_ranks, by = c("from", "variable"), relationship = "many-to-one") |>
      left_join(integrator_infos, by = "variable", relationship = "many-to-one") |>
      complete(date = seq.Date(min(date), max(date), by = "day")) |>
      mutate(
        diff = if_else((data_rank == 1L) | (data_rank < integrator_data_rank), master - !!sym(input$integratorSelect), NA),
        t = annual_index(date),
        correction = k0 + a1 * sin(t) + b1 * cos(t) + a2 * sin(2 * t) + b2 * cos(2 * t)
      )

    integrator_timespan <- m |>
      filter(!is.na(diff)) |>
      summarise(min_date = min(date, na.rm = TRUE), max_date = max(date, na.rm = TRUE))

    m |> filter(between(date, integrator_timespan$min_date, integrator_timespan$max_date))
  }) |> bindEvent(merge_data(), input$integratorSelect, ignoreNULL = FALSE, ignoreInit = TRUE)

  output$distPlot <- renderPlotly({
    cat("Redrawing plot\n")
    integ_data() |>
      group_by(variable_name) |>
      do(mafig = plot_diffs(.)) |>
      subplot(nrows = 2L)
  }) |> bindEvent(integ_data(), ignoreNULL = FALSE)

  output$mergeTable <- renderTable(
    {
      cat("Redrawing table\n")
      integ_series()
    },
    striped = TRUE,
    hover = TRUE,
    bordered = TRUE
  ) |> bindEvent(integ_series(), ignoreNULL = FALSE)
}
