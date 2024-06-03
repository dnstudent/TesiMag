library(ggplot2)
library(plotly)
library(vroom)
library(dplyr)
library(stringr)

source("R/read.R")
source("R/utils.R")

server <- function(input, output, session) {
  merge_specs <- reactive({
    read_merge_specs(input$rootPath)
  })
  
  # metadata <- reactive({
  #   read_metadata(input$rootPath)
  # })

  observeEvent(input$rootPath, {
    updateSelectInput(inputId = "masterDSSelect", choices = unique(merge_specs()$dataset))
    updateSelectInput(inputId = "masterSNSelect", choices = unique(merge_specs()$sensor_key))
  })

  integ_series <- reactive({
    cat("Recomputing integ_series\n")
    if (is.null(input$masterDSSelect) || is.null(input$masterSNSelect)) {
      return(merge_specs())
    }
    merge_specs() |>
      filter(dataset == input$masterDSSelect, sensor_key == input$masterSNSelect)
  })

  observeEvent(input$masterDSSelect, {
    updateSelectInput(inputId = "integratorSelect", choices = unique(str_c(integ_series()$from_dataset, integ_series()$from_sensor_key, sep = "/")))
  })
  observeEvent(input$masterSNSelect, {
    updateSelectInput(inputId = "integratorSelect", choices = unique(str_c(integ_series()$from_dataset, integ_series()$from_sensor_key, sep = "/")))
  })

  merge_data <- reactive({
    cat("Recomputing merge_data\n")
    read_data_tables(input$rootPath, input$masterDSSelect, input$masterSNSelect)
  })

  output$distPlot <- renderPlot({
    integrator_colname <- input$integratorSelect
    iser <- integ_series() |> unite("colnames", from_dataset, from_sensor_key, sep = "/")

    correction_coeffs <- iser |>
      filter(colnames == integrator_colname) |>
      select(variable, k0, a1, b1, a2, b2, data_rank_integr = data_rank)

    data <- merge_data() |>
      left_join(correction_coeffs, by = "variable", relationship = "many-to-many") |>
      left_join(iser |> select(variable, data_rank), by = "variable", relationship = "many-to-many") |>
      filter(data_rank <= data_rank_integr) |>
      mutate(
        diff = master - !!sym(integrator_colname),
        t = annual_index(date),
        correction = k0 + a1 * sin(t) + b1 * cos(t) + a2 * sin(2 * t) + b2 * cos(2 * t)
      )
    
    date_range <- input$date_range
    
    bps <- brushedPoints(data, input$plot_brush, xvar = "date", yvar = "diff")
    if (nrow(bps) > 0L) {
      bps <- bps |> summarise(first_date = min(date, na.rm = TRUE), last_date = max(date, na.rm = TRUE), min_diff = min(diff, na.rm = TRUE), max_diff = max(diff, na.rm = TRUE))
      data <- data |> filter(between(date, bps$first_date, bps$last_date), between(diff, bps$min_diff, bps$max_diff))
    }
      
    ggplot(data) +
      geom_point(aes(date, diff, color = factor(variable)), na.rm = TRUE) +
      geom_line(aes(date, correction, color = factor(variable))) +
      facet_grid(variable ~ ., scales = "free_y") +
      scale_color_manual(values = c("1" = "red", "-1" = "blue"))
  })

  # output$tableOut <- renderTable({
  #   integrator_colname <- input$integratorSelect
  #   master_colname <- integ_series() |> slice_min(data_rank) |> unite("colnames", from_dataset, from_sensor_key, sep = "/") |> pull(colnames) |> first()
  #
  #   merge_data() |> mutate(diff = !!sym(master_colname) - !!sym(integrator_colname)) |> slice_head(n = 5L)
  # })
}
