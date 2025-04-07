# Introduzione

## Normali Climatiche

- Caratterizzazione climatica locale e di area: dai dati grezzi all'interpolazione;
> Normale climatica come statistica su una determinata dimensione temporale (annuale, mensile) di una serie di misure di variabili meteorologiche a scala inferiore in una stessa location. Sono di fatto un riferimento di un aspetto del clima locale e possono essere quindi usate per studiare i cambiamenti del clima e fare previsioni di lungo termine in ambito di progettazione territoriale/produttiva. Il carattere aggregato delle normali le rende fortemente correlate alle proprietà topografiche locali e macroclimatiche di area. Partendo dai dati misurati dalle stazioni "in loco" e passando per una raccolta di valori puntuali si arriva ad interpolare su griglia regolare delle stime tramite metodi geostatistici.
- Requisiti di rappresentatività: copertura spaziale e temporale;
> Normali strettamente legate a proprietà topografiche quali quota, distanza dal mare, posizione a valle o in cresta, esposizione del versante... Serve un sample sufficiente di stazioni per ciascuna di queste caratteristiche. Dal punto di vista temporale esigenze di completezza: WMO suggerisce di calcolare le clino con almeno 20 anni di dati (contando gap-filling). Qui si tengono serie che forniscono almeno 5 anni.
- Requisiti di qualità dei dati e metadati: precisione, omogeneità, accuratezza.
> Come da indicazioni WMO i dati devono essere privi di errori legati al malfunzionamento degli strumenti e da segnali "artificiali" (serie omogenee). L'accuratezza della collocazione geografica dà un contributo all'interpolazione.

![Dato meteorologico giornaliero](ASSETS/introduzione/reggio_series_sample)
*Figura 1: Dato meteorologico giornaliero*

![Normali climatiche di stazione](ASSETS/introduzione/clino_jan)
*Figura 2: Normali climatiche di stazione*

![Normali climatiche di area](ASSETS/introduzione/bgkrig)
*Figura 3: Normali climatiche di area*
> Nelle immagini qualche esempio di come vengono forniti i dati, del risultato dell'aggregazione e dell'interpolazione.

## Caratteristiche del Dataset Preparato

- Temperature minime e massime giornaliere per calcolare climatologie mensili;
- Accurato controllo di qualità sia dei dati che dei metadati ed omogeneizzazione;
> Descritti meglio in seguito. Procedure sia automatiche che manuali.
- Copertura del centro-nord Italia e delle regioni alpine limitrofe (estensione al centro-sud e alle isole e alla piovosità cumulata ad opera di altri).
> Stazioni al contorno per interpolare meglio.

![Distribuzione delle serie risultanti](ASSETS/introduzione/merged_map.pdf)
*Figura 4: Distribuzione delle serie risultanti*
> Risultato in figura. Comprese solo le stazioni utilizzate per le climatologie.

## Lavori di Riferimento

Aggiornamento del dataset preparato da Brunetti et al. (2014) delle normali climatiche 1961--1990.
> Si era fatto un lavoro analogo con le banche dati allora disponibili (principalmente ex-servizio Idrografico). Nel periodo 1991-2020 si sono diffuse le stazioni automatiche e la gestione delle reti di misura è passata in mano alle regioni, con importanti conseguenze esposte dopo sulla frammentazione dei dati.

Si sono presi come riferimento, confronto e base di dati altri lavori e dataset italiani a risoluzione giornaliera:

- SCIA (ISPRA)
    - Controllato e validato (con caveat);
    - Anagrafiche non molto accurate;
    - Disponibilità di dati disomogenea, buona per il passato.
> Qualche errore evidente trovato nelle serie delle sinottiche. Anagrafiche sembrano essere state riconvertite di sistema di riferimento in vari casi. Come si vedrà più avanti, le serie registrate hanno dei "buchi" artificiali anche importanti (caso Piemonte). Circa 2500 serie per il centro-nord.

- Dataset CNR-ISAC
    - Controllato, validato, completato e omogeneizzato;
    - Serie lunghe ma in numero ridotto.
> Circa 280 serie per il centro-nord.

- Dataset DPC
    - Controllato, validato, completato e omogeneizzato;
    - Serie numerose ma dal 2002;
    - Aggregazione dati differente.
> Estremi delle medie orarie: variabile "meno estrema". Circa 2200 serie. Procedura di controllo qualità, completamento e omogeneizzazione usata come base per questo lavoro.