# Composizione del Dataset

- tre passaggi: raccolta dati, combinazione delle serie, controllo qualità ed omogeneizzazione

## Raccolta dei dati giornalieri

- La gestione delle stazioni e dei dati è affidata ad enti regionali e provinciali; (transizione competenze SIMN)
- Dati meteorologici reperiti da:
  - 11 dataset ARPA/SIR o provinciali
  - 9 dataset sovraregionali (3 italiani, 6 esteri)
- Totale: circa 12.350 serie
- in figura qualche esempio di disponibilità dei dati: quasi nessuno dei dataset sorgenti è "completo", SCIA fornisce serie di troppo.

![Disponibilità temporale di alcuni dei dataset sorgenti](ASSETS/composizione_dataset/sources_availability.png)
*Disponibilità temporale di alcuni dei dataset sorgenti*

---

- raccolta eterogenea:
    - serie duplicate, uguali o differenti;
    - poca coerenza nell'organizzazione e registrazione dei dati:
        - differenti anagrafiche delle stazioni;
        - differenti codici identificativi;
        - collocazioni geografiche ed altimetriche;
        - definizioni di "giornata di misura";
        - definizioni di estremo di temperatura;
        - finestra temporale dei dati disponibili.
    - carenza di tracce storiche, che comporta difficoltà nel distinguere se stazioni vicine sono ricollocate o meno e non aiuta l'omogeneizzazione
- elaborata procedura di merging per sintetizzare in serie rappresentative, generalmente coincidenti con le serie di stazione, l'ampio set di dati e metadati a disposizione, in particolare filtrando i dati duplicati e combinando sequenze di stazioni ricollocate "poco" o molto vicine.

---

## *Merging*: Procedura
- interpretazione del problema del merging con un grafo, i cui nodi sono le serie (dati e meta) e gli archi le relazioni di "unificabilità" tra coppie;
- per ogni regione ho stabilito dei criteri che mi permettessero di collegare con un arco (trovare un match) le coppie di sequenze originali che fanno parte della stessa serie;
- cercando le componenti connesse del grafo trovo le serie rappresentative;
- i gruppi di sequenze originali vengono unificati prendendo una serie master e integrando una alla volta le altre; l'integrazione avviene solo previo soddisfacimento di alcune condizioni di coerenza e inserisce dati corretti con un modello delle differenze tra serie master e serie integrante, in modo da omogeneizzare; l'ordine di integrazione è stato determinato in base all'affidabilità del dataset di provenienza;
- correzione manuale delle collocazioni con GE, SW e cataloghi terzi.
- fatte tutte le regioni, merging complessivo.

---

## *Merging*: Risultati

- Circa 2.500 serie italiane (più 1.680 straniere), di cui circa 2.350 con almeno 5 anni di dati: limite preso come condizione necessaria al completamento delle serie.
- Disponibilità temporale sul trentennio significativamente superiore ai dataset sorgenti, in particolare guardando al numero di serie con almeno tra 10 e 20 anni di dati. Al netto dei fatti SCIA contiene il 75% delle mensilità utili rispetto al mio ds.
- Distribuzione geografica ben rappresentativa del territorio

![Numero di serie disponibili con almeno n anni](ASSETS/composizione_dataset/improvements.png)
*Numero di serie disponibili con almeno n anni*

![Distribuzione in quota delle serie ottenute](ASSETS/composizione_dataset/elevation_distribution.png)
*Distribuzione in quota delle serie ottenute*


## Controllo qualità ed omogeneizzazione

1. Controllo degli errori grossolani (es. \(|T| > 50°C\) e dati ripetuti)
2. Confronto con serie sintetiche ERA5
3. Confronto con serie limitrofe
4. *Gap filling* dei dati mancanti

**Risultato:** Scartate 15 serie italiane al termine del quality check.

- Le stazioni automatiche non supervisionate sono note per introdurre errori importanti e sporadici dovuti a malfunzionamento degli strumenti: introducono scostamenti anche significativi ma non necessariamente assurdi rispetto al meteo reale.
- CQ: per ogni serie sono state costruite serie sintetiche di riferimento prima con ERA5 (per eliminare gli errori più consistenti e prevenire un effetto di feedback negativo nel passaggio successivo) e poi con le serie limitrofe. I dati non soddifacenti le condizioni impostate (scostamenti dalle serie di rif tra 4 e 6 °C) sono stati rimossi.
- I valori di test sono quindi stati usati per completare a livello giornaliero le serie ove appropriato (valori sufficientemente rappresentativi).