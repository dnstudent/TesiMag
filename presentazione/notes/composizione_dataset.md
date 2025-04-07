# Sorgenti

14 italiani + 6 stranieri tra dataset nazionali, regionali (ARPA e SIR) e provinciali.

![Disponibilità temporale di alcuni dei dataset sorgenti](ASSETS/composizione_dataset/sources_availability.png)
*Disponibilità temporale di alcuni dei dataset sorgenti*

---

# Sorgenti

## Disponibilità delle serie dei dataset nazionali

![Disponibilità di serie dei dataset nazionali a confronto con quello prodotto](ASSETS/composizione_dataset/monthly_availabilities.png)
*Disponibilità di serie dei dataset nazionali a confronto con quello prodotto*

## Serie di SCIA utilizzabili per il calcolo delle normali climatiche

![Serie di SCIA utilizzabili per il calcolo delle normali climatiche di temperatura 1991--2020](ASSETS/composizione_dataset/scia_disp.png)
*Serie di SCIA utilizzabili per il calcolo delle normali climatiche di temperatura 1991--2020*

---

# Merging: Procedura

Combinazione di serie singole rappresentative di una data località con strumenti della teoria dei grafi.

## Regione per regione:

- **Matching**: individuazione delle corrispondenze tra singole serie
- Raggruppamento in serie rappresentative
- Combinazione tramite modello delle differenze
- Correzione manuale delle anagrafiche

*Merging* complessivo a posteriori.

![Grafo dei match di alcune serie di Reggio Emilia](ASSETS/composizione_dataset/reggio_series_graph_pmod.pdf)
*Grafo dei match di alcune serie di Reggio Emilia*

---

# Merging: Risultati

- \(\approx 2500\) serie di cui \(\approx 2350\) con almeno 5 anni di dati: \(\approx 3\) serie ogni \(\qty{100}{\kilo\meter^2}\)
- Disponibilità temporale sul trentennio nettamente superiore ai dataset sorgenti
- Distribuzione in quota ben rappresentativa del territorio

![Numero di serie disponibili con almeno n anni](ASSETS/composizione_dataset/improvements.png)
*Numero di serie disponibili con almeno n anni*

![Distribuzione in quota delle serie ottenute](ASSETS/composizione_dataset/elevation_distribution.png)
*Distribuzione in quota delle serie ottenute*

---

# Controllo qualità

1. Controllo degli errori grezzi: \(\lvert \mathrm{T} \rvert < \qty{50}{\degreeCelsius}\) e dati ripetuti
2. Confronto con serie sintetiche ERA5: metodo delle anomalie e dei contributi relativi
3. Confronto con serie limitrofe
4. *Gap filling* dei dati mancanti