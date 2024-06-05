library(bslib)
library(shiny)

master <- layout_columns(
    selectInput("masterDSSelect", "Dataset", choices = NULL), selectInput("masterSNSelect", "Sensor key", choices = NULL)
  )

integrator <- selectInput("integratorSelect", "Serie", choices = NULL)



ui <- page_navbar(
  # Application title
  title = "Visualizzatore merging",
  nav_panel(
    "Plot",
    layout_columns(
      master, integrator
    ),
    layout_columns(plotOutput("distPlot", brush = "plot_brush")),
    # card(card_header("merge_specs"), tableOutput("tableOut"))
  ),
  nav_panel(
    "Impostazioni",
    textInput("rootPath", "Cartella del dataset", width = "100%", value = fs::path_expand("../../db/conv/merged_corrected"))
  )
  # nav_panel("Impostazioni", card(full_screen = TRUE, textInput("rootPath", "Dataset path", width = "100%", value = fs::path_expand("~/Local_Workspace/TesiMag/db/conv/merged_corrected")))),
)