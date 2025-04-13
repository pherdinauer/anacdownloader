# ANAC Dataset Downloader

Un downloader avanzato per i dataset pubblicati dall'Autorità Nazionale Anticorruzione (ANAC) su dati.anticorruzione.it.

## Caratteristiche

- Download automatico di dataset JSON e CSV
- Sistema di cache per memorizzare i dataset analizzati
- Navigazione completa di tutte le pagine di dataset
- Rilevamento avanzato di file JSON e ZIP
- Supporto per la ripresa dei download interrotti
- Monitoraggio in tempo reale del progresso con:
  - Barra di progresso grafica
  - Velocità di download
  - Tempo rimanente stimato (ETA)
  - Dimensione file
  - Contatori in tempo reale per i file trovati
- Gestione automatica degli errori con retry e backoff esponenziale
- Rate limiting per evitare sovraccarichi del server
- Logging dettagliato delle operazioni
- Interfaccia utente interattiva
- Verifica integrità della cache

## Requisiti

- Python 3.8+
- Dipendenze Python:
  - aiohttp
  - beautifulsoup4
  - bs4
  - asyncio
  - rich
  - requests
  - aiofiles
  - tqdm

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
pip install -r requirements.txt
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
   - 7. Rigenera cache
   - 8. Verifica integrità cache
   - 9. Esci

3. I file verranno scaricati nella cartella `downloads/` organizzati in sottocartelle per tipo:
   - `downloads/json/` per i file JSON
   - `downloads/csv/` per i file CSV

## Funzionalità Avanzate

### Sistema di Cache
- Memorizza tutti i dataset e i file analizzati in un file JSON (`datasets_cache.json`)
- Evita di riscansionare ripetutamente l'intero sito
- Validità configurabile della cache (default: 7 giorni)
- Possibilità di rigenerare la cache manualmente
- Verifica dell'integrità della cache

### Ricerca Avanzata di File
- Rileva automaticamente file JSON e ZIP contenenti JSON
- Supporto speciale per dataset CUP e SmartCIG
- Esplora tutte le pagine del catalogo dataset
- Naviga attraverso gruppi, risorse e pagine di organizzazioni
- Normalizzazione degli URL per evitare duplicati

### Monitoraggio Download
- Barra di progresso in tempo reale
- Velocità di download in KB/s
- Tempo rimanente stimato
- Dimensione file in formato leggibile
- Contatori in tempo reale per i file trovati durante la scansione

### Gestione Errori
- Retry automatico in caso di errori
- Backoff esponenziale tra i tentativi
- Logging dettagliato degli errori
- Possibilità di riprendere i download interrotti
- Gestione robusta di connessioni interrotte

### Rate Limiting
- Limite di richieste configurabile
- Gestione automatica dei cookie
- Simulazione comportamento browser
- Timeout configurabili
- Pausa e jitter tra le richieste

## File di Log e Stato

- `anac_downloader.log`: Log principale delle operazioni
- `download_problems.log`: Log specifico dei problemi riscontrati
- `download_state.json`: Stato dei download per la ripresa
- `datasets_cache.json`: Cache dei dataset e file analizzati
- `dataset_urls.txt`: Lista completa degli URL dei dataset trovati

## Note Tecniche

- Il programma utilizza tecniche di rate limiting per evitare di sovraccaricare il server
- I download interrotti possono essere ripresi in qualsiasi momento
- La velocità di download è limitata di default per evitare problemi
- Tutti i file scaricati vengono verificati per integrità
- Il sistema gestisce correttamente gli URL relativi e assoluti
- Supporto per strumenti di download esterni come curl e wget quando disponibili

## Licenza

Questo progetto è rilasciato sotto licenza MIT. Vedi il file LICENSE per i dettagli. 