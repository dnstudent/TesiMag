# Introduzione
- Buongiorno a tutti ecc. ecc.
- Presento il procedimento di costruzione di un dataset di variabili meterologiche a risoluzione giornaliera che ho elaborato come progetto di tesi.

## Caratteristiche del Dataset

- Temperature minime e massime giornaliere;
- Copertura del centro-nord Italia e delle regioni alpine limitrofe.
- Copertura del trentennio più recente, 1991-2020. Periodo di passaggio dalle stazioni meccaniche a quelle automatiche e di passaggio di competenze dal SIMN alle regioni.
- Orientato al calcolo delle normali climatiche mensili di stazione e su griglia: tutte le scelte e le priorità stabilite tengono conto di questo indirizzo.


## Normali Climatiche

- descrizione del clima di una località geografica, ovvero le componenti principali del segnale meteorologico, che sono fortemente correlate alle caratteristiche topografiche di un luogo;
- statistica dei dati meteorologici giornalieri: impone determinati requisiti di completezza ed omogeneità (WMO);
- due livelli: normali di stazione e di area.
- Di conseguenza: priorità alla completezza temporale e all'accuratezza dei metadati in vista del modello.

![Dato meteorologico giornaliero](ASSETS/introduzione/reggio_series_sample)
*Figura 1: Dato meteorologico giornaliero*

![Normali climatiche di stazione](ASSETS/introduzione/clino_jan)
*Figura 2: Normali climatiche di stazione*

![Normali climatiche di area](ASSETS/introduzione/bgkrig)
*Figura 3: Normali climatiche di area*
> Nelle immagini qualche esempio di come vengono forniti i dati, del risultato dell'aggregazione e dell'interpolazione.

## Lavori di Riferimento

- aggiornamento del lavoro di Brunetti et al. (2014) per le normali climatiche 1961--1990.
- esiste già SCIA, ma si parte dall'idea che sia migliorabile in quanto a completezza temporale e accuratezza delle informazioni spaziali.
