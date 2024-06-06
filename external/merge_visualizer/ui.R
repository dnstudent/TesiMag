library(bslib)
library(shiny)

ui <- page_sidebar(
  # card(card_header("Plot"), plotlyOutput("distPlot")),
  # sliderInput("plotDates", "Periodo", min = NULL, max = NULL, value = NULL),
  plotlyOutput("distPlot"),
  card(card_header("Specifiche merging"), tableOutput("mergeTable")),
  sidebar = sidebar(
    selectInput("masterDSSelect", "Dataset", choices = NULL),
    selectInput("masterSNSelect", "Sensor key", choices = NULL),
    selectInput("integratorSelect", "Serie", choices = NULL)
  )
)