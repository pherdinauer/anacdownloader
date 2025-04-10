# ANAC Dataset Downloader

Un downloader avanzato per i dataset pubblicati dall'Autorità Nazionale Anticorruzione (ANAC) su dati.anticorruzione.it.

## Caratteristiche

- Download automatico di dataset JSON e CSV
- Supporto per la ripresa dei download interrotti
- Monitoraggio in tempo reale del progresso con:
  - Barra di progresso grafica
  - Velocità di download
  - Tempo rimanente stimato (ETA)
  - Dimensione file
- Gestione automatica degli errori con retry
- Rate limiting per evitare sovraccarichi del server
- Logging dettagliato delle operazioni
- Interfaccia utente interattiva

## Requisiti

- Python 3.7+
- Dipendenze Python:
  - aiohttp
  - beautifulsoup4
  - rich

## Installazione

1. Clona il repository:
```bash
git clone https://github.com/tuouser/anacdownloader.git
cd anacdownloader
```

2. Crea un ambiente virtuale (opzionale ma consigliato):
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
```

3. Installa le dipendenze:
```bash
pip install aiohttp beautifulsoup4 rich
```

## Utilizzo

1. Avvia il programma:
```bash
python scraper.py
```

2. Scegli un'opzione dal menu:
   - 1. Scarica file JSON
   - 2. Scarica file CSV
   - 3. Scarica entrambi
   - 4. Riprendi download interrotti
   - 5. Mostra statistiche
   - 6. Scarica file specifico
   - 7. Esci

3. I file verranno scaricati nella cartella `downloads/` organizzati in sottocartelle per tipo:
   - `downloads/json/` per i file JSON
   - `downloads/csv/` per i file CSV

## Funzionalità Avanzate

### Monitoraggio Download
- Barra di progresso in tempo reale
- Velocità di download in KB/s
- Tempo rimanente stimato
- Dimensione file in formato leggibile

### Gestione Errori
- Retry automatico in caso di errori
- Backoff esponenziale tra i tentativi
- Logging dettagliato degli errori
- Possibilità di riprendere i download interrotti

### Rate Limiting
- Limite di richieste configurabile
- Gestione automatica dei cookie
- Simulazione comportamento browser
- Timeout configurabili

## File di Log

- `anac_downloader.log`: Log principale delle operazioni
- `download_problems.log`: Log specifico dei problemi riscontrati
- `download_state.json`: Stato dei download per la ripresa

## Note

- Il programma utilizza tecniche di rate limiting per evitare di sovraccaricare il server
- I download interrotti possono essere ripresi in qualsiasi momento
- La velocità di download è limitata di default per evitare problemi
- Tutti i file scaricati vengono verificati per integrità

## Licenza

Questo progetto è rilasciato sotto licenza MIT. Vedi il file LICENSE per i dettagli. 