# Pivoting both datasets to wide format, and padding missing dates. Subsequent computations will be faster.
library(tidyr)
library(dplyr)
library(padr)
library(arrow)

path_to_scia <- "..."
start_date <- "2000-01-01" # Inserisci una data d'inizio per le misure
scia <- read_parquet(path_to_scia) |>
    filter(date >= start_date) |> # Mantiene solo le misure effettuate dopo start_date
    arrange(internal_id, date) # Ordina per id e data, dovrebbe rendere più ordinato il passaggio successivo

# Libera eventuale memoria sprecata
gc()

# pivot_wider (libreria tidyr) trasforma una tabella in formato "lungo" in una tabella in formato "largo" utilizzando i valori nella colonna
# <values_from>, e mettendo come etichette di colonna i valori che sono in <names_from>
pivot_wider(scia, id_cols = date, names_from = internal_id, values_from = "Precipitazioni cumulate") |>
    pad(interval = "d") |> # Assicura che tutte le date siano presenti, anche se non ci sono dati. Riempie con NA
    write_parquet("scia_wider_precipitazioni.parquet") # Salva il tabellone in "scia_wider_precipitazioni.parquet"
