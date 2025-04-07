# Introduzione
> Buongiorno a tutti ecc. ecc.
> Presento il lavoro che ho fatto per costruire un dataset delle temperature minime e massime giornaliere per il territorio del centro-nord Italia utilizzabile come base per il calcolo di normali climatiche 1991-2020.

## Caratteristiche del Dataset

- Temperature minime e massime giornaliere;
- Copertura del centro-nord Italia e delle regioni alpine limitrofe. I dati delle regioni limitrofe rendono più accurata la procedura di interpolazione al bordo (immagine).
- Copertura del trentennio più recente, 1991-2020. Periodo di passaggio dalle stazioni meccaniche a quelle automatiche e di passaggio di competenze dal SIMN alle regioni.
- Orientato al calcolo delle normali climatiche mensili: tutte le scelte e le priorità stabilite tengono conto di questo indirizzo.


## Normali Climatiche

- descrizione del clima di un'area geografica, ovvero le componenti principali del segnale meteorologico direttamente correlate alle caratteristiche topografiche di un luogo;
- statistica dei dati meteorologici giornalieri: pone determinati requisiti di completezza (WMO);
- due livelli: normali di stazione e di area.
- Di conseguenza: priorità alla completezza temporale e all'accuratezza dei metadati in vista del modello.

![Dato meteorologico giornaliero](ASSETS/introduzione/reggio_series_sample)
*Figura 1: Dato meteorologico giornaliero*

![Normali climatiche di stazione](ASSETS/introduzione/clino_jan)
*Figura 2: Normali climatiche di stazione*

![Normali climatiche di area](ASSETS/introduzione/bgkrig)
*Figura 3: Normali climatiche di area*
> Nelle immagini qualche esempio di come vengono forniti i dati, del risultato dell'aggregazione e dell'interpolazione.



![Distribuzione delle serie risultanti](ASSETS/introduzione/merged_map.pdf)
*Figura 4: Distribuzione delle serie risultanti*
> Risultato in figura. Comprese solo le stazioni utilizzate per le climatologie.

## Lavori di Riferimento

- aggiornamento del lavoro di Brunetti et al. (2014) per le normali climatiche 1961--1990.
- esiste già SCIA, ma si parte dall'idea che sia migliorabile secondo i criteri esposti.
