library(bslib)
library(shiny)
library(plotly)

ui <- page_sidebar(
  # card(card_header("Plot"), plotlyOutput("distPlot")),
  # sliderInput("plotDates", "Periodo", min = NULL, max = NULL, value = NULL),
  plotlyOutput("distPlot"),
  card(card_header("Specifiche merging"), tableOutput("mergeTable")),
  sidebar = sidebar(
    selectizeInput("masterSelect", "Serie master", choices = NULL),
    selectInput("integratorSelect", "Serie integrante", choices = NULL)
  )
)