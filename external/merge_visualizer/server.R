library(ggplot2)
library(plotly)
library(vroom)
library(dplyr)
library(stringr)

source("R/read.R")
source("R/utils.R")

root_path <- fs::path_expand("./merged_corrected")
merge_specs <- read_merge_specs(root_path) |> relocate(merged)
# metadata <- read_metadata(root_path)
tconv <- tibble(variable = c(-1L, 1L), variable_name = factor(c("TMND", "TMXD")))

plot_diffs <- function(df) {
  df |>
    plot_ly(x = ~date, split = ~variable_name) |>
    add_trace(y = ~diff, type = "scatter", mode = "markers", color = ~variable_name, colors = c("TMND" = "blue", "TMXD" = "red")) |>
    add_trace(y = ~correction, type = "scatter", mode = "lines", color = I("gray")) |>
    layout(yaxis = list(title = "Correction"), xaxis = list(title = "Date"))
}

server <- function(input, output, session) {
  observeEvent(root_path, {
    updateSelectInput(inputId = "masterDSSelect", choices = unique(merge_specs$dataset))
    updateSelectInput(inputId = "masterSNSelect", choices = unique(merge_specs$series_key))
  })

  integ_series <- reactive({
    cat("Recomputing integ_series\n")
    merge_specs |>
      filter(dataset == input$masterDSSelect, series_key == input$masterSNSelect) |>
      arrange(data_rank)
  })

  observeEvent(input$masterDSSelect, {
    updateSelectInput(inputId = "integratorSelect", choices = unique(str_c(integ_series()$from_dataset, integ_series()$from_sensor_key, sep = "/")))
  })
  observeEvent(input$masterSNSelect, {
    updateSelectInput(inputId = "integratorSelect", choices = unique(str_c(integ_series()$from_dataset, integ_series()$from_sensor_key, sep = "/")))
  })

  merge_data <- reactive({
    cat("Recomputing merge_data\n")
    req(input$masterDSSelect, input$masterSNSelect)
    read_data_tables(root_path, input$masterDSSelect, input$masterSNSelect) |> left_join(tconv, by = "variable")
  })

  integ_data <- reactive({
    cat("Recomputing integ_data\n")
    req(input$masterDSSelect, input$masterSNSelect, input$integratorSelect)

    data_ranks <- integ_series() |> select(from_dataset, from_sensor_key, variable, data_rank)
    integrator_infos <- integ_series() |>
      unite(series_tag, from_dataset, from_sensor_key, sep = "/") |>
      filter(series_tag == input$integratorSelect) |>
      select(variable, k0, a1, b1, a2, b2, integrator_data_rank = data_rank)

    m <- merge_data() |>
      left_join(data_ranks, by = c("from_dataset", "from_sensor_key", "variable"), relationship = "many-to-one") |>
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
  })

  output$distPlot <- renderPlotly({
    cat("Redrawing plot\n")
    integ_data() |>
      group_by(variable_name) |>
      do(mafig = plot_diffs(.)) |>
      subplot(nrows = 2L)
  })

  output$mergeTable <- renderTable(
    {
      integ_series()
    },
    striped = TRUE,
    hover = TRUE,
    bordered = TRUE
  )
}
