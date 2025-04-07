# Composizione del Dataset

- tre passaggi

## Raccolta dei dati giornalieri

### Esigenze:
- rappresentatività temporale e spaziale per il trentennio 1991--2020
> è stata raccolta la maggior quantità di dati reperibile, ma dove questo risultasse troppo oneroso (case Dext3r ER) ci si è limitati al periodo in esame.
- Completezza dei metadati e accuratezza delle collocazioni
> Un'interpolazione efficace beneficia di collocazioni precise, dal momento che le variabili topografiche di supporto vengono estratte da un DEM e/o da altri prodotti geospaziali.
- Possibilità di aggiornamento futuro
> Procedura di richiesta dati automatizzata: sono stati conservati gli identificativi delle serie, in modo da poter aggiornare la base di dati in caso di bisogno. Alcune regioni hanno iniziato ad implementare interfacce automatiche efficaci open data.

Risorse: numerosi dataset regionali e nazionali ben forniti ed ``accessibili''.

### Problemi riscontrati:
- disomogeneità e obsolescenza delle interfacce di accesso ai dati
> Costituisce un onere significativo nel lavoro di ricerca ed analisi del clima. Con il passaggio di competenze dal Servizio Idrografico e Mareografico Nazionale alle regioni sono state sbloccate maggiori risorse per la gestione delle reti, ma manca un coordinamento nazionale che renda agevole l'accesso agli stessi. Questo implica che rispetto al passato è disponibile una maggior mole di dati con una minore coerenza. Il sistema SCIA (come del resto questo lavoro) è un tentativo a posteriori di far fronte a questa situazione, con una serie di limiti.
- disomogeneità e incoerenze tra dataset diversi
> - differenti anagrafiche delle stazioni;
> - differenti codici identificativi;
> - collocazioni geografiche ed altimetriche;
> - definizioni di "giornata di misura";
> - definizioni di estremo di temperatura;
> - finestra temporale dei dati disponibili.
- Serie duplicate
> Sia inter- che intra-dataset, talvolta riportando dati differenti.
- Generale carenza di tracce storiche delle stazioni
> Complica l'analisi di omogeneità delle serie e la rilevazione dei duplicati. è spesso difficile capire se due serie vicine sono differenti o un ricollocamento, anche perché talvolta sono completate dal gestore.

**Soluzione:** *Merging* intra- e inter-dataset e controllo manuale delle collocazioni dubbie.
> è stata sviluppata una procedura articolata per combinare i dati recuperati allo scopo di filtrare quelli in eccesso e ottenere serie lunghe e rappresentative delle singole località. In quest'ottica serie "spezzate" per (probabile) ricollocamento nelle vicinanze sono state combinate, così come serie talmente vicine da essere ipoteticamente le stesse (casi cittadini).

---

## Sorgenti

14 italiani + 6 stranieri tra dataset nazionali, regionali (ARPA e SIR) e provinciali.

![Disponibilità temporale di alcuni dei dataset sorgenti](ASSETS/composizione_dataset/sources_availability.png)
*Disponibilità temporale di alcuni dei dataset sorgenti*

> A parte quelli nazionali citati si sono recuperati per quasi ogni regione (eccetto VDA) i dataset ARPA (o analoghi). Il *data exploring* ha rivelato che generalmente:
> - SCIA ha una superiore disponibilità di dati pre-2000;
> - ARPA hanno superiore disponibilità post-2000 e metadati generalmente più accurati ed esaustivi;
> - DPC è l'ultima risorsa;

---

## Sorgenti

### Disponibilità delle serie dei dataset nazionali
![Disponibilità di serie dei dataset nazionali a confronto con quello prodotto](ASSETS/composizione_dataset/monthly_availabilities.png)
*Disponibilità di serie dei dataset nazionali a confronto con quello prodotto*

### Serie di SCIA utilizzabili
![Serie di SCIA utilizzabili per il calcolo delle normali climatiche di temperatura 1991–2020](ASSETS/composizione_dataset/scia_disp.png)
*Serie di SCIA utilizzabili per il calcolo delle normali climatiche di temperatura 1991–2020*

> In diapositiva i trend di disponibilità dei dataset nazionali: a sinistra il numero di serie, mese per mese, con il numero di giorni sufficiente a contribuire al calcolo della normale. A destra le serie effettivamente utilizzate da ISPRA per il calcolo delle normali: in alcune zone complesse (e.g. Lombardia, Liguria, Toscana) sono decisamente poche.

---

## *Merging*: Procedura

Combinazione di serie singole rappresentative di una data località con strumenti della teoria dei grafi.

- Regione per regione:
    - **Matching**: individuazione delle corrispondenze tra singole serie
    - Raggruppamento in serie rappresentative
    - Combinazione tramite modello delle differenze
    - Correzione manuale delle anagrafiche
- Merging complessivo a posteriori

> L'insieme delle serie raccolte e la loro combinabilità secondo i criteri indicati possono essere pensati come nodi ed archi di un grafo, le cui componenti connesse rappresentano le serie unificate (illustrare figura). Buona parte del lavoro è stato trovare empiricamente delle buone definizioni formali delle relazioni di match tra coppie di serie. Nello specifico si sono costruiti alberi decisionali su parametri numerici atti a descrivere la somiglianza tra le serie sia sul fronte dei dati che su quello dei metadati (percentuale di dati identici, differenza media di temperatura, somiglianza dei nominativi, distanza tra le posizioni dichiarate, differenza tra quote...). L'approccio scelto ha consentito di non dover definire alla perfezione tali relazioni (sarebbe troppo articolato), come si può vedere in figura.
> I gruppi di serie sono stati combinati e integrati secondo un ordine specifico ai dati e ai metadati
> L'integrazione è avvenuta prendendo una serie "master" e aggiungendo iterativamente i dati delle serie integranti prese una alla volta qualora aggiungessero più di due anni di dati. L'inserimento avviene modificando i dati in ingresso con una correzione modellizzata sulle differenze tra master e integrante come tre armoniche o la media delle differenze, a seconda della disponibilità di dati.
> La correzione manuale dei metadati è stata fatta con Google Earth, Street View e cataloghi di terze parti.

![Grafo dei match di alcune serie di Reggio Emilia](ASSETS/composizione_dataset/reggio_series_graph_pmod.pdf)
*Grafo dei match di alcune serie di Reggio Emilia*

---

## *Merging*: Risultati

- ~2500 serie di cui ~2350 con almeno 5 anni di dati, per ~3 serie ogni 100 km²
- Disponibilità temporale sul trentennio significativamente superiore ai dataset sorgenti
- Distribuzione in quota ben rappresentativa del territorio

![Numero di serie disponibili con almeno n anni](ASSETS/composizione_dataset/improvements.png)
*Numero di serie disponibili con almeno n anni*

![Distribuzione in quota delle serie ottenute](ASSETS/composizione_dataset/elevation_distribution.png)
*Distribuzione in quota delle serie ottenute*
> Sul limite scelto dei 5 anni sono ~ 300 in più rispetto a SCIA, dove comunque un certo numero è ripetuto. In termini di mensilità "complete" SCIA ne ha circa il 75%. Integrazioni soprattuto in Lombardia, Trentino e Toscana
---

## Controllo qualità

1. Controllo degli errori grezzi: \(|T| < 50°C\) e dati ripetuti
2. Confronto con serie sintetiche ERA5: metodo delle anomalie
3. Confronto con serie limitrofe
4. *Gap filling* dei dati mancanti

> Le stazioni automatiche non supervisionate sono note per introdurre errori importanti e sporadici dovuti a malfunzionamento degli strumenti: introducono scostamenti anche significativi ma non necessariamente assurdi rispetto al meteo reale.
> CQ: per ogni serie sono state costruite serie sintetiche di riferimento prima con ERA5 (per eliminare gli errori più consistenti e prevenire un effetto di feedback negativo nel passaggio successivo) e poi con le serie limitrofe. I dati non soddifacenti le condizioni impostate (scostamenti dalle serie di rif tra 4 e 6 °C) sono stati rimossi.
> I valori di test sono quindi stati usati per completare a livello giornaliero le serie ove appropriato (valori sufficientemente rappresentativi).