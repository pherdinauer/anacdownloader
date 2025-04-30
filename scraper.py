import os
import logging
import hashlib
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple
import re
import asyncio
import aiohttp
from pathlib import Path
import json
from datetime import datetime
import math
import time
import subprocess
import sys
import platform
import random
import traceback
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.logging import RichHandler
from rich.table import Table

# Configurazione del logging
def setup_logging():
    # Crea la directory logs se non esiste
    os.makedirs("logs", exist_ok=True)
    
    # Nome del file di log con timestamp
    log_filename = f"logs/anac_downloader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configura il logger principale
    logger = logging.getLogger("anac_downloader")
    logger.setLevel(logging.DEBUG)
    
    # Handler per il file di log
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s\n%(pathname)s:%(lineno)d\n%(funcName)s\n',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Handler per la console con Rich
    console_handler = RichHandler(rich_tracebacks=True)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    
    return logger

# Logger globale
logger = setup_logging()

class AdvancedLogger:
    def __init__(self, log_file: str = "anac_downloader.log"):
        self.logger = logger
        self.detailed_log_file = "logs/download_details.log"
        self.error_log_file = "logs/errors.log"
        
        # Crea i file di log se non esistono
        os.makedirs("logs", exist_ok=True)
        if not os.path.exists(self.detailed_log_file):
            with open(self.detailed_log_file, 'w') as f:
                f.write("Timestamp,Operation,File,URL,Size,Status,Duration,Speed,Notes\n")
        if not os.path.exists(self.error_log_file):
            with open(self.error_log_file, 'w') as f:
                f.write("Timestamp,Error,File,URL,Stack Trace\n")
    
    def _log_to_detailed_file(self, operation, filename, url="", size=0, status="", duration=0, speed=0, notes=""):
        """Log dettagliato delle operazioni di download"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.detailed_log_file, 'a', encoding='utf-8') as f:
            f.write(f"{timestamp},{operation},{filename},{url},{size},{status},{duration:.2f},{speed:.2f},{notes}\n")
    
    def _log_error(self, error, filename="", url="", stack_trace=""):
        """Log dettagliato degli errori"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.error_log_file, 'a', encoding='utf-8') as f:
            f.write(f"{timestamp},{error},{filename},{url},{stack_trace}\n")
    
    def log_download_start(self, filename: str, size: int, url: str = "", dataset_name: str = "", file_number: int = 0, total_files: int = 0):
        """Log dell'inizio di un download"""
        self.logger.info(f"Starting download: {filename} ({size/1024/1024:.1f} MB)")
        self._log_to_detailed_file(
            "START",
            filename,
            url,
            size,
            "STARTED",
            0,
            0,
            f"Dataset: {dataset_name}, File {file_number}/{total_files}"
        )
    
    def log_download_progress(self, filename: str, current: int, total: int, speed: float, elapsed: float):
        """Log del progresso di un download"""
        progress = (current / total) * 100 if total > 0 else 0
        self.logger.debug(f"Progress: {filename} - {progress:.1f}% ({current}/{total} bytes) - {speed/1024/1024:.1f} MB/s")
    
    def log_download_complete(self, filename: str, duration: float):
        """Log del completamento di un download"""
        self.logger.info(f"✓ Download completed: {filename} in {duration:.1f}s")
        self._log_to_detailed_file(
            "COMPLETE",
            filename,
            "",
            0,
            "COMPLETED",
            duration,
            0,
            "Download completed successfully"
        )
    
    def log_download_error(self, filename: str, error: str, url: str = ""):
        """Log di un errore durante il download"""
        self.logger.error(f"✗ Download failed: {filename} - {error}")
        self._log_to_detailed_file(
            "ERROR",
            filename,
            url,
            0,
            "FAILED",
            0,
            0,
            error
        )
        self._log_error(
            error,
            filename,
            url,
            traceback.format_exc()
        )
    
    def log_chunk_status(self, filename: str, chunk_num: int, total_chunks: int, status: str):
        """Log dello stato di un chunk di download"""
        self.logger.debug(f"Chunk {chunk_num}/{total_chunks} of {filename}: {status}")
    
    def log_retry(self, filename: str, attempt: int, max_attempts: int, delay: float):
        """Log di un tentativo di download"""
        self.logger.warning(f"Retry {attempt}/{max_attempts} for {filename} after {delay:.1f}s")
        self._log_to_detailed_file(
            "RETRY",
            filename,
            "",
            0,
            "RETRYING",
            0,
            0,
            f"Attempt {attempt}/{max_attempts}"
        )
    
    def log_file_info(self, filename: str, url: str, size: int, file_type: str, dataset_url: str = ""):
        """Log delle informazioni di un file"""
        self.logger.info(f"File info: {filename} ({size/1024/1024:.1f} MB) - {file_type}")
        self._log_to_detailed_file(
            "INFO",
            filename,
            url,
            size,
            "INFO",
            0,
            0,
            f"Type: {file_type}, Dataset: {dataset_url}"
        )

    def warning(self, message: str):
        """Log di un warning"""
        self.logger.warning(message)
        self._log_to_detailed_file(
            "WARNING",
            "",
            "",
            0,
            "WARNING",
            0,
            0,
            message
        )

    def error(self, message: str):
        """Log di un errore generico"""
        self.logger.error(message)
        self._log_to_detailed_file(
            "ERROR",
            "",
            "",
            0,
            "ERROR",
            0,
            0,
            message
        )

    def info(self, message: str):
        """Log di un messaggio informativo"""
        self.logger.info(message)
        self._log_to_detailed_file(
            "INFO",
            "",
            "",
            0,
            "INFO",
            0,
            0,
            message
        )

class RateLimiter:
    def __init__(self, calls_per_second=1):
        self.calls_per_second = calls_per_second
        self.last_call = 0
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        async with self.lock:
            now = time.time()
            if self.last_call > 0:
                time_since_last = now - self.last_call
                if time_since_last < 1.0 / self.calls_per_second:
                    await asyncio.sleep(1.0 / self.calls_per_second - time_since_last)
            self.last_call = time.time()

class ANACScraper:
    """Classe per lo scraping dei dataset ANAC."""
    
    def __init__(self):
        """Inizializza lo scraper."""
        self.base_url = "https://dati.anticorruzione.it"
        self.session = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://dati.anticorruzione.it/opendata/dataset',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.logger = AdvancedLogger()
        self.max_retries = 5
        self.min_file_size = 1024  # 1KB minimo
        self.timeout = aiohttp.ClientTimeout(total=1800)  # 30 minuti di timeout
        self.chunk_size = 1024 * 1024  # 1MB per chunk
        self.num_connections = 1  # Ridotto a 1 - approccio più conservativo 
        self.rate_limiter = RateLimiter(calls_per_second=0.2)  # Ridotto a 1 richiesta ogni 5 secondi
        
        # Configurazione per il backoff dinamico dei chunk
        self.chunk_backoff = {
            'initial_delay': 0.1,  # 100ms iniziale
            'max_delay': 30,      # massimo 30 secondi
            'multiplier': 2,      # raddoppia ad ogni fallimento
            'jitter': 0.1         # 10% di jitter
        }
        
        # File per il log dei problemi e lo stato dei download
        self.problem_log_file = "download_problems.log"
        self.state_file = "download_state.json"
        self.load_state()
        
        # Cookie storage
        self.cookies = {}
        
        # Download options (can be overridden per download)
        self.download_options = {
            'limit_rate': 200,  # Ridotto a 200 KB/s default
            'retries': 15,      # Aumentato a 15
            'retry_delay': 30,  # Aumentato a 30 secondi
            'timeout': 7200,    # Mantenuto a 2 ore
            'connect_timeout': 60,
            'num_connections': 1,  # Ridotto a 1 connessione di default
            'segment_size': 5 * 1024 * 1024  # 5MB per segmento (piccoli segmenti)
        }
        
        self.USE_EXTERNAL = True  # Flag to use external tools
        self._check_external_tools()
    
    async def __aenter__(self):
        """Support for async context manager protocol."""
        # Inizializzazione della sessione
        if self.session is None:
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=self.timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup when exiting the async context manager."""
        # Pulizia delle risorse
        if hasattr(self, 'session') and self.session is not None:
            await self.session.close()
            self.session = None
        return False  # Non sopprimere le eccezioni
        
    async def _make_request(self, relative_url: str) -> str:
        """Effettua una richiesta HTTP GET e restituisce il contenuto HTML.
        
        Args:
            relative_url: URL relativo (senza il dominio base).
            
        Returns:
            Il contenuto HTML della risposta.
        """
        # Correggi URL relativi vs assoluti
        if relative_url.startswith('http'):
            # È già un URL completo
            url = relative_url
        else:
            # Assicurati che l'URL relativo inizi con /
            if not relative_url.startswith('/'):
                relative_url = f"/{relative_url}"
            url = f"{self.base_url}{relative_url}"
            
        self.logger.logger.debug(f"Richiesta HTTP a: {url}")
        
        # Assicuriamo che la sessione esista
        if self.session is None:
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=self.timeout)
        
        # Applicazione del rate limiting
        await self.rate_limiter.acquire()
        
        max_retries = 5
        retry_count = 0
        backoff_time = 2
        
        while retry_count < max_retries:
            try:
                async with self.session.get(url, timeout=self.timeout) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 429:  # Too Many Requests
                        retry_count += 1
                        wait_time = backoff_time * (2 ** retry_count) + random.uniform(0, 1)
                        self.logger.logger.warning(f"Rate limite raggiunto, attendo {wait_time:.2f}s prima di riprovare...")
                        await asyncio.sleep(wait_time)
                    else:
                        self.logger.logger.error(f"Errore nella richiesta HTTP: {response.status} - {url}")
                        return ""
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                retry_count += 1
                wait_time = backoff_time * (2 ** retry_count) + random.uniform(0, 1)
                self.logger.logger.error(f"Errore di connessione: {str(e)}. Tentativo {retry_count}/{max_retries}, attendo {wait_time:.2f}s...")
                await asyncio.sleep(wait_time)
        
        self.logger.logger.error(f"Tutti i tentativi falliti per {url}")
        return ""
    
    def _check_external_tools(self):
        """Check if curl or wget are available on the system."""
        self.curl_available = False
        self.wget_available = False
        
        # Check for curl
        try:
            subprocess.run(["curl", "--version"], 
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE, 
                          check=True)
            self.curl_available = True
        except (subprocess.SubprocessError, FileNotFoundError):
            pass
            
        # Check for wget
        try:
            subprocess.run(["wget", "--version"], 
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE, 
                          check=True)
            self.wget_available = True
        except (subprocess.SubprocessError, FileNotFoundError):
            pass
            
        if not (self.curl_available or self.wget_available):
            self.USE_EXTERNAL = False
            print("Nessuno strumento di download esterno (curl/wget) trovato. Utilizzo aiohttp.")
        else:
            tools = []
            if self.curl_available:
                tools.append("curl")
            if self.wget_available:
                tools.append("wget")
            print(f"Utilizzo {', '.join(tools)} per i download.")
    
    def _get_downloader(self):
        """Determina quale downloader usare (curl o wget)."""
        if platform.system() == "Windows":
            # Su Windows, cerca curl.exe che è incluso in Windows 10
            if os.path.exists("C:\\Windows\\System32\\curl.exe"):
                return "curl"
            else:
                raise RuntimeError("curl.exe non trovato. Installa curl o aggiungi il percorso alle variabili di sistema.")
        else:
            # Su Linux/Mac, cerca curl o wget
            curl_path = subprocess.run(['which', 'curl'], capture_output=True, text=True).stdout.strip()
            if curl_path:
                return "curl"
            wget_path = subprocess.run(['which', 'wget'], capture_output=True, text=True).stdout.strip()
            if wget_path:
                return "wget"
            raise RuntimeError("Nessun downloader (curl o wget) trovato. Installa curl o wget.")

    def load_state(self):
        """Carica lo stato dei download precedenti."""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    self.download_state = json.load(f)
                    # Assicurati che tutte le chiavi necessarie esistano
                    if 'partial_downloads' not in self.download_state:
                        self.download_state['partial_downloads'] = {}
                    if 'completed_files' not in self.download_state:
                        self.download_state['completed_files'] = []
                    if 'failed_files' not in self.download_state:
                        self.download_state['failed_files'] = []
                    if 'last_update' not in self.download_state:
                        self.download_state['last_update'] = None
            else:
                self.download_state = {
                    'completed_files': [],
                    'failed_files': [],
                    'partial_downloads': {},
                    'last_update': None
                }
        except Exception as e:
            self.logger.error(f"Errore nel caricamento dello stato: {str(e)}")
            self.download_state = {
                'completed_files': [],
                'failed_files': [],
                'partial_downloads': {},
                'last_update': None
            }
            
    def save_state(self):
        """Salva lo stato corrente dei download."""
        self.download_state['last_update'] = datetime.now().isoformat()
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.download_state, f, indent=2)
        except Exception as e:
            self.logger.error(f"Errore nel salvataggio dello stato: {str(e)}")
        
    def log_problem(self, message: str):
        """Logga un problema nel file dedicato."""
        with open(self.problem_log_file, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} - {message}\n")
            
    async def get_file_size(self, url: str) -> int:
        """Ottiene la dimensione totale del file."""
        try:
            async with self.session.head(url, timeout=self.timeout) as response:
                if response.status == 200:
                    return int(response.headers.get('content-length', 0))
                return 0
        except Exception as e:
            self.logger.error(f"Errore nel recupero della dimensione del file: {str(e)}")
            return 0

    def _get_curl_command(self, url: str, output_file: str, resume: bool = True) -> list[str]:
        cmd = [
            "curl",
            "--retry", "5",              # Retry 5 times
            "--retry-delay", "10",       # Wait 10 seconds between retries
            "--retry-max-time", "60",    # Maximum time for retries
            "--connect-timeout", "30",   # Connection timeout
            "--max-time", "3600",        # Maximum total time
            "--limit-rate", "50k",       # Limit download speed to 50KB/s
            "--keepalive-time", "60",    # Keep-alive for 60 seconds
            "--compressed",              # Request compressed response
            "--location",                # Follow redirects
            "--fail",                    # Fail on HTTP errors
            "--silent",                  # Silent mode
            "--show-error",             # Show errors
            "-H", "User-Agent: Mozilla/5.0",
            "-H", "Accept: */*",
            "-H", "Connection: keep-alive",
            url,
            "--output", output_file
        ]
        if resume and os.path.exists(output_file):
            cmd.append("--continue-at", "-")
        return cmd

    async def _fast_download(self, url: str, output_file: str, file_size: int) -> bool:
        """Tenta un download veloce senza limitazioni di velocità."""
        try:
            self.logger.logger.info(f"Tentativo download veloce per {os.path.basename(output_file)} ({self._format_size(file_size)})")
            
            # Creazione directory se non esiste
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Prepara il comando curl con progress bar
            cmd = [
                "curl",
                "--retry", "2",              # Solo 2 retry
                "--retry-delay", "5",        # Attesa breve tra i retry
                "--connect-timeout", "15",   # Timeout connessione ridotto
                "--max-time", "1800",        # 30 minuti massimo
                "--compressed",              # Compressione
                "--location",                # Segui redirect
                "--progress-bar",            # Mostra barra di progresso
                "--show-error",              # Mostra errori
                "-H", "User-Agent: Mozilla/5.0",
                "-H", "Accept: */*",
                "-H", "Connection: keep-alive",
                url,
                "--output", output_file
            ]
            
            # Esegui curl e mostra l'output in tempo reale
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Monitora il progresso
            start_time = time.time()
            last_update = start_time
            last_percentage = 0
            last_bytes = 0
            
            while True:
                # Leggi l'output di curl
                line = process.stderr.readline()
                if not line and process.poll() is not None:
                    break
                    
                if line:
                    line = line.strip()
                    
                    # Estrai informazioni dalla barra di progresso di curl
                    if '%' in line:
                        try:
                            # Estrai percentuale
                            parts = line.split()
                            current_percentage = 0
                            
                            for part in parts:
                                if '%' in part:
                                    current_percentage = float(part.replace('%', ''))
                            
                            # Calcola i byte scaricati basandosi sulla percentuale
                            current_bytes = (current_percentage / 100) * file_size
                            
                            # Calcola la velocità solo se abbiamo abbastanza dati
                            if current_bytes > last_bytes and time.time() - last_update > 0:
                                speed = (current_bytes - last_bytes) / (time.time() - last_update) / (1024 * 1024)  # MB/s
                                last_bytes = current_bytes
                                last_update = time.time()
                            else:
                                speed = 0
                            
                            # Aggiorna solo se la percentuale è cambiata significativamente
                            if abs(current_percentage - last_percentage) >= 1 or time.time() - last_update >= 1:
                                # Calcola ETA
                                elapsed = time.time() - start_time
                                if current_percentage > 0 and elapsed > 0:
                                    remaining = (100 - current_percentage) * elapsed / current_percentage
                                    eta = f"{int(remaining/60)}m {int(remaining%60)}s"
                                    
                                    # Stampa il progresso
                                    print(f"\rProgresso: {current_percentage:.1f}% - Velocità: {speed:.1f} MB/s - ETA: {eta}", end='', flush=True)
                                    
                                    last_percentage = current_percentage
                        except:
                            pass
            
            print()  # Nuova linea dopo il completamento
            
            stdout, stderr = process.communicate()
            
            if process.returncode == 0 and os.path.exists(output_file):
                actual_size = os.path.getsize(output_file)
                if actual_size >= file_size * 0.99:  # Permette una differenza dell'1%
                    self.logger.logger.info(f"Download veloce completato: {os.path.basename(output_file)}")
                    return True
                else:
                    self.logger.logger.warning(f"Dimensione file non corrispondente. Prevista: {file_size}, Attuale: {actual_size}")
            else:
                error_msg = stderr if stderr else "Errore sconosciuto"
                self.logger.logger.warning(f"Download veloce fallito: {error_msg}")
            
            return False
        except Exception as e:
            self.logger.logger.warning(f"Errore nel download veloce: {str(e)}")
            return False

    def _format_size(self, size_bytes):
        """Formatta i byte in un formato leggibile."""
        if size_bytes < 1024:
            return f"{size_bytes} bytes"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes/(1024*1024):.1f} MB"
        else:
            return f"{size_bytes/(1024*1024*1024):.1f} GB"

    async def _prepare_for_download(self, url: str) -> None:
        """Prepara la sessione per il download simulando un browser."""
        try:
            self.logger.info(f"Preparazione per il download da: {url}")
            
            # Prima visita la pagina principale
            main_page_url = self.base_url + "/opendata/dataset"
            self.logger.info(f"Visita pagina principale: {main_page_url}")
            await self.rate_limiter.acquire()
            async with self.session.get(main_page_url, timeout=self.timeout) as response:
                self.logger.info(f"Risposta pagina principale: {response.status}")
                await response.text()
                
                # Salva i cookie
                if response.cookies:
                    for cookie in response.cookies.values():
                        self.cookies[cookie.key] = cookie.value
            
            # Poi visita la pagina del dataset
            dataset_url = url.split('/download/')[0]
            if not dataset_url.startswith('http'):
                dataset_url = f"{self.base_url}{dataset_url}"
                
            self.logger.info(f"Visita pagina dataset: {dataset_url}")
            await self.rate_limiter.acquire()
            
            headers = self.headers.copy()
            headers['Referer'] = main_page_url
            
            async with self.session.get(dataset_url, headers=headers, timeout=self.timeout, cookies=self.cookies) as response:
                self.logger.info(f"Risposta pagina dataset: {response.status}")
                await response.text()
                
                # Aggiorna i cookie
                if response.cookies:
                    for cookie in response.cookies.values():
                        self.cookies[cookie.key] = cookie.value
            
            # Aggiorna l'header Referer per il download
            self.headers['Referer'] = dataset_url
            
            # Attendi un po' prima di procedere con il download
            await asyncio.sleep(3)
            
        except Exception as e:
            self.logger.error(f"Errore nella preparazione per il download: {str(e)}")

    async def _browser_download(self, url: str, output_file: str, file_size: int) -> bool:
        """Download tramite aiohttp simulando un browser."""
        try:
            self.logger.info(f"Inizio download via browser per {os.path.basename(output_file)} ({self._format_size(file_size)})")
            
            # Assicurati che la directory esista
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Prepara la sessione simulando la navigazione di un browser
            await self._prepare_for_download(url)
            
            # Configura gli header per il download
            headers = self.headers.copy()
            headers['Accept'] = '*/*'  # Accetta qualsiasi tipo di contenuto per il download
            
            # Applica rate limiting
            await self.rate_limiter.acquire()
            
            # Apri il file per la scrittura
            with open(output_file, 'wb') as fd:
                async with self.session.get(url, headers=headers, timeout=self.timeout, cookies=self.cookies) as response:
                    if response.status != 200:
                        self.logger.error(f"Risposta non valida: {response.status}")
                        return False
                    
                    content_length = int(response.headers.get('Content-Length', '0'))
                    if content_length > 0:
                        self.logger.info(f"Dimensione file da scaricare: {self._format_size(content_length)}")
                    
                    # Leggi e scrivi a blocchi
                    bytes_downloaded = 0
                    start_time = time.time()
                    async for chunk in response.content.iter_chunked(self.chunk_size):
                        if chunk:
                            fd.write(chunk)
                            bytes_downloaded += len(chunk)
                            
                            # Mostra progresso ogni 5MB
                            if bytes_downloaded % (5 * 1024 * 1024) < self.chunk_size:
                                elapsed = time.time() - start_time
                                if elapsed > 0:
                                    speed = bytes_downloaded / elapsed / 1024  # KB/s
                                    percentage = bytes_downloaded / content_length * 100 if content_length > 0 else 0
                                    self.logger.info(f"Scaricato: {self._format_size(bytes_downloaded)} ({percentage:.1f}%) - {speed:.1f} KB/s")
            
            # Verifica dimensione finale
            if os.path.exists(output_file):
                actual_size = os.path.getsize(output_file)
                if actual_size >= file_size * 0.99:  # Tollera differenza dell'1%
                    self.logger.info(f"Download browser completato: {os.path.basename(output_file)}")
                    return True
                else:
                    self.logger.warning(f"Dimensione file non corrispondente. Prevista: {file_size}, Attuale: {actual_size}")
                    if actual_size > 1024:  # Se ha scaricato qualcosa di sensato, verifica il contenuto
                        with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read(1000)  # Leggi i primi 1000 caratteri
                            if "<html" in content and "Request Rejected" in content:
                                self.logger.error("Download bloccato dal WAF!")
            
            return False
            
        except Exception as e:
            self.logger.error(f"Errore nel download via browser: {str(e)}")
            return False

    async def _download_in_chunks(self, url: str, output_file: str, file_size: int) -> bool:
        """Scarica un file in blocchi utilizzando header Range."""
        try:
            self.logger.info(f"Download a blocchi per {os.path.basename(output_file)} ({self._format_size(file_size)})")
            
            # Crea la directory se non esiste
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Prepara la sessione con la navigazione
            await self._prepare_for_download(url)
            
            # Dimensione di ogni blocco (5MB)
            chunk_size = 5 * 1024 * 1024
            
            # Calcola il numero di blocchi
            num_chunks = math.ceil(file_size / chunk_size)
            self.logger.info(f"File diviso in {num_chunks} blocchi da {chunk_size/1024/1024:.1f}MB")
            
            # File temporaneo per i blocchi
            temp_file = output_file + ".part"
            
            # Se il file temporaneo esiste già, verifica quali parti abbiamo
            downloaded_size = 0
            if os.path.exists(temp_file):
                downloaded_size = os.path.getsize(temp_file)
                self.logger.info(f"Trovato file parziale di {self._format_size(downloaded_size)}, continuo da lì")
            
            # Inizia dall'ultimo byte scaricato
            start_byte = downloaded_size
            
            # Apri il file in modalità append binaria
            with open(temp_file, 'ab') as f:
                for chunk_num in range(start_byte // chunk_size, num_chunks):
                    # Calcola l'intervallo di byte per questo blocco
                    chunk_start = chunk_num * chunk_size
                    chunk_end = min(chunk_start + chunk_size - 1, file_size - 1)
                    
                    # Massimo 3 tentativi per blocco
                    for attempt in range(3):
                        try:
                            self.logger.info(f"Scarico blocco {chunk_num + 1}/{num_chunks} ({self._format_size(chunk_end - chunk_start + 1)})")
                            
                            # Applica rate limiting
                            await self.rate_limiter.acquire()
                            
                            # Crea gli header con l'intervallo di byte
                            headers = self.headers.copy()
                            headers['Range'] = f'bytes={chunk_start}-{chunk_end}'
                            headers['Accept'] = '*/*'
                            
                            # Effettua la richiesta
                            async with self.session.get(url, headers=headers, timeout=self.timeout, cookies=self.cookies) as response:
                                # Verifica se la risposta è corretta
                                if response.status not in (200, 206):
                                    self.logger.error(f"Errore nella risposta: {response.status}")
                                    if attempt < 2:
                                        await asyncio.sleep(5 * (attempt + 1))
                                        continue
                                    else:
                                        return False
                                
                                # Leggi e scrivi i dati
                                data = await response.read()
                                
                                # Verifica che abbiamo ottenuto la dimensione corretta
                                if len(data) != (chunk_end - chunk_start + 1):
                                    self.logger.warning(f"Dimensione blocco non corrispondente. Prevista: {chunk_end - chunk_start + 1}, Ricevuta: {len(data)}")
                                    if attempt < 2:
                                        await asyncio.sleep(5 * (attempt + 1))
                                        continue
                                
                                # Scrivi il blocco
                                f.write(data)
                                f.flush()
                                
                                # Aggiorna lo stato di avanzamento
                                current_size = chunk_end + 1
                                percentage = (current_size / file_size) * 100
                                self.logger.info(f"Progresso: {self._format_size(current_size)} / {self._format_size(file_size)} ({percentage:.1f}%)")
                                
                                # Successo, passa al blocco successivo
                                break
                        except Exception as e:
                            self.logger.error(f"Errore nel download del blocco {chunk_num + 1}: {str(e)}")
                            if attempt < 2:
                                await asyncio.sleep(5 * (attempt + 1))
                            else:
                                return False
            
            # Verifica dimensione finale
            if os.path.exists(temp_file):
                final_size = os.path.getsize(temp_file)
                if final_size >= file_size * 0.99:  # Tollera una differenza dell'1%
                    # Rinomina il file temporaneo al nome finale
                    os.rename(temp_file, output_file)
                    self.logger.info(f"Download a blocchi completato: {os.path.basename(output_file)}")
                    return True
                else:
                    self.logger.error(f"Dimensione finale non corrispondente. Prevista: {file_size}, Attuale: {final_size}")
            
            return False
        except Exception as e:
            self.logger.error(f"Errore nel download a blocchi: {str(e)}")
            return False

    async def download_file_in_segments(self, url: str, output_file: str, file_size: int) -> bool:
        """Scarica un file in segmenti, salvando il progresso e riprendendo se necessario."""
        try:
            # Crea la directory se non esiste
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Dimensione iniziale del segmento basata sulla dimensione totale del file
            if file_size < 100 * 1024 * 1024:  # < 100MB
                initial_segment_size = 10 * 1024 * 1024  # 10MB
            elif file_size < 1024 * 1024 * 1024:  # < 1GB
                initial_segment_size = 25 * 1024 * 1024  # 25MB
            else:  # >= 1GB
                initial_segment_size = 50 * 1024 * 1024  # 50MB
                
            # Dimensione minima e massima del segmento
            min_segment_size = 1 * 1024 * 1024  # 1MB
            max_segment_size = 100 * 1024 * 1024  # 100MB
            current_segment_size = min(initial_segment_size, max_segment_size)
            
            # Contatore per errori consecutivi
            consecutive_errors = 0
            max_consecutive_errors = 3  # Numero massimo di errori consecutivi prima di ridurre la dimensione
            
            # Nome file temporaneo per il download parziale
            temp_file = output_file + ".part"
            
            # Verifica se esiste già un file parziale
            start_position = 0
            if os.path.exists(temp_file):
                start_position = os.path.getsize(temp_file)
                self.logger.info(f"Trovato download parziale: {start_position/(1024*1024):.1f}MB, riprendo da lì")
            
            # Calcola quanti segmenti devono essere scaricati
            total_segments = math.ceil(file_size / current_segment_size)
            start_segment = start_position // current_segment_size
            
            self.logger.info(f"Download segmentato per {os.path.basename(output_file)} ({file_size/(1024*1024):.1f}MB) - {total_segments} segmenti totali da {current_segment_size/(1024*1024):.1f}MB")
            
            # Salva l'informazione nei download parziali
            filename = os.path.basename(output_file)
            self.download_state['partial_downloads'][filename] = {
                'url': url,
                'total_bytes': file_size,
                'downloaded_bytes': start_position,
                'last_update': datetime.now().isoformat()
            }
            self.save_state()
            
            # Prepara la sessione simulando un browser
            await self._prepare_for_download(url)
            
            # Apri il file in modalità append binaria
            with open(temp_file, 'ab') as f:
                # Scarica i segmenti rimanenti
                for segment_idx in range(start_segment, total_segments):
                    # Calcola i byte di inizio e fine per questo segmento
                    segment_start = segment_idx * current_segment_size
                    segment_end = min(segment_start + current_segment_size - 1, file_size - 1)
                    
                    # Massimo 5 tentativi per segmento
                    for attempt in range(5):
                        try:
                            self.logger.info(f"Scarico segmento {segment_idx+1}/{total_segments} (byte {segment_start}-{segment_end})")
                            
                            # Applica rate limiting
                            await self.rate_limiter.acquire()
                            
                            # Imposta gli header per questo segmento
                            headers = self.headers.copy()
                            headers['Range'] = f'bytes={segment_start}-{segment_end}'
                            headers['Accept'] = '*/*'
                            
                            # Effettua la richiesta
                            async with self.session.get(url, headers=headers, timeout=self.timeout, cookies=self.cookies) as response:
                                if response.status not in (200, 206):  # 206 = Partial Content
                                    self.logger.error(f"Risposta non valida ({response.status}) per il segmento {segment_idx+1}")
                                    consecutive_errors += 1
                                    if attempt < 4:  # Riprova se non è l'ultimo tentativo
                                        await asyncio.sleep(5 * (attempt + 1))
                                        continue
                                    # Se fallisce dopo tutti i tentativi, riduci la dimensione del segmento solo se abbiamo avuto molti errori consecutivi
                                    if consecutive_errors >= max_consecutive_errors and current_segment_size > min_segment_size:
                                        current_segment_size = max(current_segment_size // 2, min_segment_size)
                                        self.logger.warning(f"Riduco dimensione segmento a {current_segment_size/(1024*1024):.1f}MB dopo {consecutive_errors} errori consecutivi")
                                        # Ricalcola i segmenti rimanenti
                                        total_segments = math.ceil((file_size - segment_start) / current_segment_size)
                                        segment_idx -= 1  # Riprova lo stesso segmento con dimensione ridotta
                                        consecutive_errors = 0  # Reset errori consecutivi
                                        break
                                    return False
                                
                                # Leggi i dati di questo segmento
                                data = await response.read()
                                expected_size = segment_end - segment_start + 1
                                
                                if len(data) != expected_size:
                                    self.logger.warning(f"Dimensioni segmento non corrispondono: previsto {expected_size}, ricevuto {len(data)}")
                                    consecutive_errors += 1
                                    if attempt < 4:  # Riprova se non è l'ultimo tentativo
                                        await asyncio.sleep(5 * (attempt + 1))
                                        continue
                                    # Se fallisce dopo tutti i tentativi, riduci la dimensione del segmento solo se abbiamo avuto molti errori consecutivi
                                    if consecutive_errors >= max_consecutive_errors and current_segment_size > min_segment_size:
                                        current_segment_size = max(current_segment_size // 2, min_segment_size)
                                        self.logger.warning(f"Riduco dimensione segmento a {current_segment_size/(1024*1024):.1f}MB dopo {consecutive_errors} errori consecutivi")
                                        # Ricalcola i segmenti rimanenti
                                        total_segments = math.ceil((file_size - segment_start) / current_segment_size)
                                        segment_idx -= 1  # Riprova lo stesso segmento con dimensione ridotta
                                        consecutive_errors = 0  # Reset errori consecutivi
                                        break
                                
                                # Scrivi i dati nel file e va avanti
                                f.write(data)
                                f.flush()
                                
                                # Aggiorna lo stato del download
                                current_size = segment_end + 1
                                percentage = (current_size / file_size) * 100
                                self.download_state['partial_downloads'][filename]['downloaded_bytes'] = current_size
                                self.download_state['partial_downloads'][filename]['last_update'] = datetime.now().isoformat()
                                self.save_state()
                                
                                self.logger.info(f"Progresso: {current_size/(1024*1024):.1f}MB / {file_size/(1024*1024):.1f}MB ({percentage:.1f}%)")
                                
                                # Se il download è riuscito, prova ad aumentare la dimensione del segmento
                                if current_segment_size < initial_segment_size:
                                    new_segment_size = min(current_segment_size * 2, initial_segment_size)
                                    if new_segment_size > current_segment_size:
                                        current_segment_size = new_segment_size
                                        self.logger.info(f"Aumento dimensione segmento a {current_segment_size/(1024*1024):.1f}MB")
                                        # Ricalcola i segmenti rimanenti
                                        total_segments = math.ceil((file_size - current_size) / current_segment_size)
                                
                                # Reset errori consecutivi dopo un successo
                                consecutive_errors = 0
                                
                                # Successo per questo segmento, interrompi il ciclo di tentativi
                                break
                                
                        except asyncio.TimeoutError:
                            self.logger.warning(f"Timeout durante il download del segmento {segment_idx+1}")
                            consecutive_errors += 1
                            if attempt < 4:  # Riprova se non è l'ultimo tentativo
                                await asyncio.sleep(5 * (attempt + 1))
                                continue
                            # Se fallisce dopo tutti i tentativi, riduci la dimensione del segmento solo se abbiamo avuto molti errori consecutivi
                            if consecutive_errors >= max_consecutive_errors and current_segment_size > min_segment_size:
                                current_segment_size = max(current_segment_size // 2, min_segment_size)
                                self.logger.warning(f"Riduco dimensione segmento a {current_segment_size/(1024*1024):.1f}MB dopo {consecutive_errors} errori consecutivi")
                                # Ricalcola i segmenti rimanenti
                                total_segments = math.ceil((file_size - segment_start) / current_segment_size)
                                segment_idx -= 1  # Riprova lo stesso segmento con dimensione ridotta
                                consecutive_errors = 0  # Reset errori consecutivi
                                break
                            return False
                        except Exception as e:
                            self.logger.error(f"Errore durante il download del segmento {segment_idx+1}: {str(e)}")
                            consecutive_errors += 1
                            if attempt < 4:  # Riprova se non è l'ultimo tentativo
                                await asyncio.sleep(5 * (attempt + 1))
                                continue
                            # Se fallisce dopo tutti i tentativi, riduci la dimensione del segmento solo se abbiamo avuto molti errori consecutivi
                            if consecutive_errors >= max_consecutive_errors and current_segment_size > min_segment_size:
                                current_segment_size = max(current_segment_size // 2, min_segment_size)
                                self.logger.warning(f"Riduco dimensione segmento a {current_segment_size/(1024*1024):.1f}MB dopo {consecutive_errors} errori consecutivi")
                                # Ricalcola i segmenti rimanenti
                                total_segments = math.ceil((file_size - segment_start) / current_segment_size)
                                segment_idx -= 1  # Riprova lo stesso segmento con dimensione ridotta
                                consecutive_errors = 0  # Reset errori consecutivi
                                break
                            return False
            
            # Verifica dimensione finale
            if os.path.exists(temp_file):
                actual_size = os.path.getsize(temp_file)
                if actual_size >= file_size * 0.99:  # Permette una differenza dell'1%
                    # Verifica se il file di destinazione esiste già
                    if os.path.exists(output_file):
                        try:
                            self.logger.info(f"Il file {output_file} esiste già. Tentativo di rimozione...")
                            os.remove(output_file)
                        except Exception as e:
                            self.logger.error(f"Impossibile rimuovere il file esistente: {str(e)}")
                            # Se non possiamo rimuovere il file esistente, rinomina il file temporaneo con un altro nome
                            alternative_output = output_file + ".new"
                            os.rename(temp_file, alternative_output)
                            self.logger.info(f"File rinominato come {alternative_output} invece di {output_file}")
                            output_file = alternative_output
                    
                    try:
                        # Rinomina il file temporaneo al nome finale
                        os.rename(temp_file, output_file)
                        
                        # Aggiorna lo stato dei download
                        filename = os.path.basename(output_file)
                        if filename in self.download_state['partial_downloads']:
                            del self.download_state['partial_downloads'][filename]
                        if filename not in self.download_state['completed_files']:
                            self.download_state['completed_files'].append(filename)
                        if filename in self.download_state['failed_files']:
                            self.download_state['failed_files'].remove(filename)
                        self.save_state()
                        
                        self.logger.info(f"Download completato: {os.path.basename(output_file)}")
                        return True
                    except Exception as e:
                        self.logger.error(f"Errore nel rinominare il file temporaneo: {str(e)}")
                        return False
                else:
                    self.logger.error(f"Dimensione file non corrispondente. Prevista: {file_size}, Attuale: {actual_size}")
            
            return False
            
        except Exception as e:
            self.logger.error(f"Errore nel download a segmenti: {str(e)}")
            return False

    async def _download_chunk_with_curl(self, url, output_file, start_byte, end_byte, segment_idx, chunk_idx, limit_rate, mode='ab'):
        """Download a small chunk using curl with very restrictive parameters."""
        try:
            temp_chunk = f"{output_file}.chunk{chunk_idx}"
            
            cmd = [
                "curl", "-L", "-o", temp_chunk, 
                "--retry", "3",              # Fewer retries per chunk
                "--retry-delay", "5",
                "--connect-timeout", "30",
                "--max-time", "300",         # 5 minute max per chunk
                "--limit-rate", f"{limit_rate}k",
                "-H", f"User-Agent: {self.headers['User-Agent']}",
                "-H", f"Range: bytes={start_byte}-{end_byte}",
                "-H", "Accept: */*",
                "-H", "Connection: keep-alive",
            ]
            
            # Add cookie file if available
            cookie_file = None
            if self.cookies:
                try:
                    cookie_file = f".curl_cookies_seg{segment_idx}_chunk{chunk_idx}.txt"
                    with open(cookie_file, 'w') as f:
                        for name, value in self.cookies.items():
                            f.write(f"dati.anticorruzione.it\tTRUE\t/\tFALSE\t0\t{name}\t{value}\n")
                    cmd.extend(["--cookie", cookie_file])
                except Exception as e:
                    self.logger.error(f"Errore cookie per chunk: {str(e)}")
            
            cmd.append(url)
            
            # Execute curl
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            return_code = await asyncio.wait_for(process.wait(), timeout=600)
            
            # Clean up cookie file
            if cookie_file and os.path.exists(cookie_file):
                try:
                    os.remove(cookie_file)
                except:
                    pass
            
            if return_code == 0 and os.path.exists(temp_chunk):
                # Verify chunk size
                chunk_size = os.path.getsize(temp_chunk)
                expected_size = end_byte - start_byte + 1
                
                if abs(chunk_size - expected_size) <= 1024:  # Allow 1KB difference
                    # Append to main segment file
                    with open(temp_chunk, 'rb') as infile, open(output_file, mode) as outfile:
                        if mode == 'ab':
                            outfile.seek(0, 2)  # Seek to end
                        else:
                            outfile.seek(start_byte, 0)  # Seek to position
                        outfile.write(infile.read())
                    
                    # Remove temporary chunk file
                    try:
                        os.remove(temp_chunk)
                    except:
                        pass
                    
                    return True
                else:
                    print(f"Dimensione chunk non corrisponde: atteso {expected_size}, ottenuto {chunk_size}")
            
            # Clean up temp file
            if os.path.exists(temp_chunk):
                try:
                    os.remove(temp_chunk)
                except:
                    pass
            
            return False
        except Exception as e:
            print(f"Errore in _download_chunk_with_curl: {str(e)}")
            return False
            
    async def download_file(self, url: str, output_file: str, file_size: int) -> bool:
        """Download a file with progress tracking and retry logic."""
        try:
            # Verifica se il file esiste già con la dimensione corretta
            if os.path.exists(output_file):
                current_size = os.path.getsize(output_file)
                # Se la dimensione è simile a quella attesa, considera il file già scaricato
                if abs(current_size - file_size) <= 1024 or (current_size > 0 and file_size == 0):
                    self.logger.logger.info(f"File {os.path.basename(output_file)} esiste già con dimensione corretta. Download saltato.")
                    return True
                else:
                    self.logger.logger.info(f"File {os.path.basename(output_file)} esiste ma dimensione diversa (attuale: {current_size}, attesa: {file_size}). Tentativo download veloce.")
            
            # Assicurati che la directory esista
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Prima prova con download veloce (curl/wget)
            if self.USE_EXTERNAL:
                self.logger.logger.info(f"Tentativo download veloce per {os.path.basename(output_file)}")
                if await self._fast_download(url, output_file, file_size):
                    return True
                self.logger.logger.warning(f"Download veloce fallito, tentativo con browser...")
            
            # Se il download veloce fallisce, prova con il browser
            if await self._browser_download(url, output_file, file_size):
                return True
            self.logger.logger.warning(f"Download browser fallito, tentativo con segmenti...")
            
            # Se entrambi i metodi falliscono, usa il download a segmenti
            return await self.download_file_in_segments(url, output_file, file_size)
                
        except Exception as e:
            self.logger.logger.error(f"Errore nel download con aiohttp: {str(e)}")
            return False

    async def get_json_files(self, dataset_url: str) -> List[Dict[str, str]]:
        """Recupera tutti i file JSON da una pagina dataset."""
        self.logger.logger.info(f"Recupero file JSON da {dataset_url}")
        json_files = []
        
        try:
            # Prepara headers migliori per evitare il blocco anti-scraping
            custom_headers = self.headers.copy()
            custom_headers['Referer'] = 'https://dati.anticorruzione.it/opendata/dataset'
            custom_headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
            custom_headers['Accept-Language'] = 'it-IT,it;q=0.8,en-US;q=0.5,en;q=0.3'
            
            # Ottieni il contenuto della pagina
            self.logger.logger.info(f"Accesso alla pagina dataset: {dataset_url}")
            html_content = await self._make_request(dataset_url)
            if not html_content:
                self.logger.logger.error(f"Nessun contenuto ricevuto dalla pagina {dataset_url}")
                return []
                
            # Pausa per evitare di sovraccaricare il server
            await asyncio.sleep(2)
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Funzione per normalizzare gli URL
            def normalize_url(url):
                # Se l'URL già inizia con http, è già assoluto
                if url.startswith('http'):
                    return url
                # Altrimenti, aggiungi il dominio base
                if url.startswith('/'):
                    return f"{self.base_url}{url}"
                # Se è relativo, aggiungi il percorso base
                return f"{self.base_url}/{url}"
            
            # Cerca specificamente i link alle risorse (il focus è qui!)
            resource_links = soup.select('li.resource-item a.resource-url-analytics, a.btn-primary')
            self.logger.logger.info(f"Trovati {len(resource_links)} link a risorse principali")
            
            # Se non troviamo risorse con i selettori specifici, proviamo un approccio più ampio
            if not resource_links:
                resource_links = soup.select('a[href*="/resource/"]')
                self.logger.logger.info(f"Secondo tentativo: trovati {len(resource_links)} link a risorse")
            
            # Analizza ogni risorsa trovata
            for resource_link in resource_links:
                href = resource_link.get('href', '')
                if not href:
                    continue
                    
                # Verifica se è un link a una pagina di risorse
                if '/resource/' in href and not href.lower().endswith(('.json', '.zip')):
                    resource_url = normalize_url(href)
                    self.logger.logger.info(f"Accesso alla pagina risorsa: {resource_url}")
                    
                    try:
                        # Aggiungi un delay prima di ogni richiesta
                        await asyncio.sleep(3)
                        
                        # Accedi alla pagina della risorsa
                        resource_content = await self._make_request(resource_url)
                        resource_soup = BeautifulSoup(resource_content, 'html.parser')
                        
                        # Cerca il link di download diretto nella pagina della risorsa
                        download_links = resource_soup.select('a.resource-url-analytics, a.btn-primary, p.muted a[href$=".json"], p.muted a[href$=".zip"]')
                        
                        for download_link in download_links:
                            download_href = download_link.get('href', '')
                            if download_href and (download_href.lower().endswith('.json') or download_href.lower().endswith('.zip')):
                                # Questo è un link diretto al file
                                file_url = normalize_url(download_href)
                                filename = os.path.basename(file_url)
                                
                                self.logger.logger.info(f"Trovato link download: {file_url}")
                                
                                # Ottieni la dimensione del file
                                try:
                                    # Aggiungi un delay prima di verificare la dimensione
                                    await asyncio.sleep(2)
                                    file_size = await self.get_file_size(file_url)
                                    self.logger.log_file_info(filename, file_url, file_size, "JSON", resource_url)
                                except Exception as e:
                                    self.logger.logger.error(f"Errore nel recupero della dimensione del file {filename}: {str(e)}")
                                    file_size = 1  # Imposta un valore positivo anche se non riusciamo a determinare la dimensione
                                
                                json_files.append({
                                    'url': file_url,
                                    'filename': filename,
                                    'size': file_size,
                                    'dataset': os.path.basename(dataset_url)
                                })
                    except Exception as e:
                        self.logger.logger.error(f"Errore nell'accesso alla pagina risorsa {resource_url}: {str(e)}")
                
                # Controllo per link diretti a file
                elif href.lower().endswith(('.json', '.zip')):
                    file_url = normalize_url(href)
                    filename = os.path.basename(file_url)
                    
                    self.logger.logger.info(f"Trovato link diretto a file: {file_url}")
                    
                    # Ottieni la dimensione del file
                    try:
                        await asyncio.sleep(2)
                        file_size = await self.get_file_size(file_url)
                        self.logger.log_file_info(filename, file_url, file_size, "JSON", dataset_url)
                    except Exception as e:
                        self.logger.logger.error(f"Errore nel recupero della dimensione del file {filename}: {str(e)}")
                        file_size = 1  # Imposta un valore positivo anche se non riusciamo a determinare la dimensione
                    
                    json_files.append({
                        'url': file_url,
                        'filename': filename,
                        'size': file_size,
                        'dataset': os.path.basename(dataset_url)
                    })
            
            # Se non abbiamo trovato file, prova a cercare link diretti nella pagina
            if not json_files:
                self.logger.logger.info("Tentativo finale: cercando link diretti a file JSON/ZIP nella pagina")
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if href.lower().endswith(('.json', '.zip')) and self._is_json_or_zip_file(href):
                        file_url = normalize_url(href)
                        filename = os.path.basename(file_url)
                        
                        self.logger.logger.info(f"Trovato link diretto: {file_url}")
                        
                        try:
                            await asyncio.sleep(2)
                            file_size = await self.get_file_size(file_url)
                            self.logger.log_file_info(filename, file_url, file_size, "JSON", dataset_url)
                        except Exception as e:
                            self.logger.logger.error(f"Errore nel recupero della dimensione del file {filename}: {str(e)}")
                            file_size = 1
                        
                        json_files.append({
                            'url': file_url,
                            'filename': filename,
                            'size': file_size,
                            'dataset': os.path.basename(dataset_url)
                        })
            
            # Rimuovi duplicati basati sull'URL
            unique_json_files = []
            seen_urls = set()
            for file_info in json_files:
                if file_info['url'] not in seen_urls:
                    seen_urls.add(file_info['url'])
                    unique_json_files.append(file_info)
            
            return unique_json_files
            
        except Exception as e:
            self.logger.logger.error(f"Errore nel recupero dei file JSON da {dataset_url}: {str(e)}")
            return []

    async def get_csv_files(self, dataset_url: str) -> List[Dict[str, str]]:
        """Recupera tutti i file CSV da una pagina dataset."""
        self.logger.logger.info(f"Recupero file CSV da {dataset_url}")
        csv_files = []
        
        try:
            # Ottieni il contenuto della pagina
            html_content = await self._make_request(dataset_url)
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Funzione per normalizzare gli URL
            def normalize_url(url):
                # Se l'URL già inizia con http, è già assoluto
                if url.startswith('http'):
                    return url
                # Altrimenti, aggiungi il dominio base
                if url.startswith('/'):
                    return f"{self.base_url}{url}"
                # Se è relativo, aggiungi il percorso base
                return f"{self.base_url}/{url}"
            
            # Cerca link diretti a file CSV
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.lower().endswith('.csv'):
                    absolute_url = normalize_url(href)
                    filename = os.path.basename(absolute_url)
                    
                    # Ottieni la dimensione del file
                    try:
                        file_size = await self.get_file_size(absolute_url)
                        self.logger.log_file_info(filename, absolute_url, file_size, "CSV", dataset_url)
                    except Exception as e:
                        self.logger.logger.error(f"Errore nel recupero della dimensione del file {filename}: {str(e)}")
                        file_size = 0
                    
                    csv_files.append({
                        'url': absolute_url,
                        'filename': filename,
                        'size': file_size
                    })
            
            # Cerca in pagine di risorse
            resource_links = soup.find_all('a', href=lambda x: x and 'resource' in x.lower())
            for resource_link in resource_links:
                resource_url = normalize_url(resource_link['href'])
                try:
                    resource_content = await self._make_request(resource_url)
                    resource_soup = BeautifulSoup(resource_content, 'html.parser')
                    
                    # Cerca link di download nella pagina della risorsa
                    for download_link in resource_soup.find_all('a', href=True):
                        href = download_link['href']
                        if href.lower().endswith('.csv'):
                            absolute_url = normalize_url(href)
                            filename = os.path.basename(absolute_url)
                            
                            # Ottieni la dimensione del file
                            try:
                                file_size = await self.get_file_size(absolute_url)
                                self.logger.log_file_info(filename, absolute_url, file_size, "CSV", dataset_url)
                            except Exception as e:
                                self.logger.logger.error(f"Errore nel recupero della dimensione del file {filename}: {str(e)}")
                                file_size = 0
                            
                            csv_files.append({
                                'url': absolute_url,
                                'filename': filename,
                                'size': file_size
                            })
                except Exception as e:
                    self.logger.logger.error(f"Errore nel recupero della pagina risorsa {resource_url}: {str(e)}")
            
            # Rimuovi duplicati mantenendo il primo URL per ogni nome file
            seen_filenames = set()
            unique_csv_files = []
            for file_info in csv_files:
                if file_info['filename'] not in seen_filenames:
                    seen_filenames.add(file_info['filename'])
                    unique_csv_files.append(file_info)
            
            self.logger.logger.info(f"Trovati {len(unique_csv_files)} file CSV unici")
            return unique_csv_files
            
        except Exception as e:
            self.logger.logger.error(f"Errore nel recupero dei file CSV da {dataset_url}: {str(e)}")
            return []

    def _is_json_or_zip_file(self, href):
        """Verifica se l'URL è un file JSON o ZIP valido."""
        # Ignora link con parametri query o frammenti che spesso sono link di navigazione
        if not href or '?' in href or '#' in href or 'mailto:' in href:
            return False
            
        href_lower = href.lower()
        
        # Verifica se termina con estensioni valide
        is_valid_extension = href_lower.endswith('.json') or href_lower.endswith('.zip')
        
        # Verifica che non sia un link di navigazione o una pagina
        invalid_patterns = [
            'home', 'index', 'dataset', 'opendata', 'privacy', 'note-legali', 
            'copyright', 'accessibilit', 'cookies', 'content', 'organization',
            'regpia', 'aca', 'rpct', 'l190', 'anticorruzione', 'mailto'
        ]
        
        for pattern in invalid_patterns:
            if pattern in href_lower:
                return False
                
        # Ulteriore verifica che sia un nome file valido
        basename = os.path.basename(href)
        
        # Deve avere un nome file non vuoto e estensione valida
        return basename and is_valid_extension and len(basename) > 5 and not basename.startswith('#')

    async def process_downloaded_file(self, file_path):
        """Processa un file scaricato.
        
        In precedenza questo metodo estraeva i file ZIP, ora li mantiene così come sono.
        """
        # Non fare nulla, mantieni i file compressi
        return

    async def get_dataset_pages(self) -> List[str]:
        """Recupera tutte le pagine dataset dal sito ANAC."""
        self.logger.logger.info("Recupero di tutti i dataset...")
        
        # Pagina principale dei dataset
        base_dataset_url = f"{self.base_url}/opendata/dataset"
        self.logger.logger.info(f"Accesso alla pagina principale: {base_dataset_url}")
        
        dataset_pages = []
        page_num = 1
        
        while True:
            # Costruisci l'URL della pagina corrente
            if page_num == 1:
                page_url = base_dataset_url
            else:
                page_url = f"{base_dataset_url}?page={page_num}"
                
            self.logger.logger.info(f"Recupero pagina {page_num}: {page_url}")
            
            # Applica rate limiting
            await self.rate_limiter.acquire()
            
            # Ottieni il contenuto HTML
            html_content = await self._make_request(page_url)
            if not html_content:
                self.logger.logger.warning(f"Nessun contenuto ricevuto dalla pagina {page_num}. Interruzione.")
                break
                
            # Analizza il contenuto con BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Cerca specificamente i dataset validi (elementi con classe dataset-item)
            dataset_items = soup.find_all('li', class_='dataset-item')
            
            # Se non ci sono dataset in questa pagina, probabilmente abbiamo finito
            if not dataset_items:
                self.logger.logger.info(f"Nessun dataset trovato nella pagina {page_num}. Interruzione.")
                break
                
            # Estrai gli URL dei dataset da ogni elemento dataset-item
            dataset_count = 0
            for item in dataset_items:
                # Trova il link al dataset nella sezione heading
                dataset_link = item.select_one('h3.dataset-heading a')
                if dataset_link and dataset_link.has_attr('href'):
                    href = dataset_link['href']
                    # Assicurati che sia un URL di dataset valido
                    if href and '/dataset/' in href:
                        # Normalizza l'URL
                        if not href.startswith('http'):
                            if href.startswith('/'):
                                href = f"{self.base_url}{href}"
                            else:
                                href = f"{self.base_url}/{href}"
                        
                        # Aggiungi all'elenco se non è già presente
                        if href not in dataset_pages:
                            dataset_pages.append(href)
                            dataset_count += 1
            
            self.logger.logger.info(f"Trovati {dataset_count} dataset validi nella pagina {page_num}")
            
            # Verifica se esiste una pagina successiva
            next_page = soup.find('a', href=lambda href: href and f'?page={page_num+1}' in href)
            if not next_page:
                self.logger.logger.info(f"Nessuna pagina successiva trovata dopo la pagina {page_num}. Interruzione.")
                break
                
            # Passa alla pagina successiva
            page_num += 1
            
            # Breve pausa tra le pagine
            await asyncio.sleep(2)
            
        self.logger.logger.info(f"Recupero completato. Trovati {len(dataset_pages)} dataset totali.")
        return dataset_pages

def save_crash_report(error, traceback_info, cache_data=None):
    """Salva un report dettagliato quando il programma crasha."""
    try:
        # Crea la directory crash_reports se non esiste
        os.makedirs("crash_reports", exist_ok=True)
        
        # Nome del file con timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        crash_file = f"crash_reports/crash_report_{timestamp}.txt"
        
        with open(crash_file, 'w', encoding='utf-8') as f:
            f.write("=== CRASH REPORT ===\n\n")
            f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Error: {str(error)}\n\n")
            
            f.write("=== TRACEBACK ===\n")
            f.write(traceback_info)
            f.write("\n\n")
            
            f.write("=== SYSTEM INFO ===\n")
            f.write(f"OS: {platform.system()} {platform.release()}\n")
            f.write(f"Python Version: {sys.version}\n")
            f.write(f"Current Directory: {os.getcwd()}\n")
            f.write(f"Script Path: {os.path.abspath(__file__)}\n\n")
            
            if cache_data:
                f.write("=== CACHE INFO ===\n")
                f.write(f"Cache File Exists: {os.path.exists('datasets_cache.json')}\n")
                if os.path.exists('datasets_cache.json'):
                    cache_size = os.path.getsize('datasets_cache.json')
                    f.write(f"Cache File Size: {cache_size} bytes\n")
                f.write(f"Number of Datasets in Cache: {len(cache_data.get('datasets', {}))}\n")
                f.write("\nCache Structure:\n")
                for dataset_url, dataset_info in cache_data.get('datasets', {}).items():
                    f.write(f"\nDataset: {dataset_url}\n")
                    if 'json_files' in dataset_info:
                        f.write(f"  JSON Files: {len(dataset_info['json_files'])}\n")
                    if 'csv_files' in dataset_info:
                        f.write(f"  CSV Files: {len(dataset_info['csv_files'])}\n")
            
            f.write("\n=== ENVIRONMENT VARIABLES ===\n")
            for key, value in os.environ.items():
                if any(sensitive in key.lower() for sensitive in ['key', 'pass', 'secret', 'token']):
                    f.write(f"{key}: [REDACTED]\n")
                else:
                    f.write(f"{key}: {value}\n")
        
        print(f"\nIl programma è crashato. Un report dettagliato è stato salvato in: {crash_file}")
        print("Per favore, condividi questo file per aiutare a diagnosticare il problema.")
        
    except Exception as e:
        print(f"\nErrore durante il salvataggio del crash report: {str(e)}")
        print("Traceback originale:")
        print(traceback_info)

def calculate_file_hash(file_path: str) -> str:
    """Calcola l'hash SHA256 di un file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            # Leggi il file a blocchi per gestire file grandi
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Errore nel calcolo dell'hash del file {file_path}: {str(e)}")
        return ""

def scan_existing_files(download_dir: str) -> Dict[str, Dict[str, any]]:
    """Scansiona i file esistenti nella directory di download e restituisce un dizionario con i nomi dei file e le loro informazioni."""
    existing_files = {}
    try:
        if os.path.exists(download_dir):
            # Ottieni la lista dei file
            files = os.listdir(download_dir)
            total_files = len(files)
            print(f"\nTrovati {total_files} file da scansionare in {download_dir}")
            
            # Scansiona ogni file
            for i, filename in enumerate(files, 1):
                file_path = os.path.join(download_dir, filename)
                if os.path.isfile(file_path):
                    try:
                        print(f"[{i}/{total_files}] Scansione file: {filename}")
                        file_size = os.path.getsize(file_path)
                        print(f"  Dimensione: {file_size/1024/1024:.2f} MB")
                        
                        print("  Calcolo hash...", end="", flush=True)
                        file_hash = calculate_file_hash(file_path)
                        print(" completato")
                        
                        existing_files[filename] = {
                            'size': file_size,
                            'hash': file_hash,
                            'path': file_path
                        }
                        print(f"  File aggiunto al catalogo: {filename}\n")
                    except Exception as e:
                        print(f"\n  [ERRORE] Errore nella scansione del file {filename}: {str(e)}")
                        continue
        else:
            print(f"\nLa directory {download_dir} non esiste")
    except Exception as e:
        print(f"\n[ERRORE] Errore nella scansione dei file esistenti: {str(e)}")
    return existing_files

async def verify_file_integrity(file_path: str, expected_size: int) -> bool:
    """Verifica l'integrità di un file."""
    try:
        # Verifica dimensione
        if not os.path.exists(file_path):
            return False
            
        actual_size = os.path.getsize(file_path)
        if abs(actual_size - expected_size) > 1024:  # Tollera 1KB di differenza
            logger.warning(f"Dimensione file non corrispondente. Prevista: {expected_size}, Attuale: {actual_size}")
            return False
            
        # Verifica che il file non sia corrotto
        # Per file JSON, verifica che sia un JSON valido
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Leggi solo i primi 1000 caratteri per verificare il formato
                content = f.read(1000)
                if not content.strip().startswith('{') and not content.strip().startswith('['):
                    logger.warning(f"File non sembra essere un JSON valido: {file_path}")
                    return False
        except Exception as e:
            logger.error(f"Errore nella verifica del formato JSON: {str(e)}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Errore nella verifica dell'integrità del file {file_path}: {str(e)}")
        return False

async def main():
    """Funzione principale per il download dei dataset."""
    cache_data = None
    try:
        # Crea le directory necessarie
        os.makedirs('downloads/json', exist_ok=True)
        os.makedirs('downloads/csv', exist_ok=True)
        
        # Inizializza il logger
        logger = AdvancedLogger()
        
        # Scansiona i file esistenti
        print("\n=== SCANSIONE FILE ESISTENTI ===")
        print("Scansione directory JSON...")
        logger.info("Scansione dei file esistenti...")
        
        # Scansiona i file JSON
        existing_json_files = scan_existing_files('downloads/json')
        print("\nScansione directory CSV...")
        
        # Scansiona i file CSV
        existing_csv_files = scan_existing_files('downloads/csv')
        
        # Mostra il riepilogo
        print("\n=== RIEPILOGO FILE TROVATI ===")
        print(f"File JSON trovati: {len(existing_json_files)}")
        print(f"File CSV trovati: {len(existing_csv_files)}")
        print("=" * 30)
        
        logger.info(f"Trovati {len(existing_json_files)} file JSON e {len(existing_csv_files)} file CSV esistenti")
        
        # Carica o crea il file di cache
        cache_file = 'datasets_cache.json'
        cache_data = {}
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                logger.info("Cache caricata con successo")
            except Exception as e:
                logger.error(f"Errore nel caricamento della cache: {str(e)}")
                cache_data = {}
        
        # Verifica se la cache è valida
        is_cache_valid = False
        if cache_data and 'datasets' in cache_data:
            is_cache_valid = True
            logger.info(f"Cache valida con {len(cache_data['datasets'])} dataset")
        
        # Se la cache non è valida, recupera la lista dei dataset
        if not is_cache_valid:
            logger.info("Cache non valida o non presente, recupero lista dataset...")
            dataset_pages = await get_dataset_pages()
            if not dataset_pages:
                logger.error("Impossibile recuperare la lista dei dataset")
                return
            logger.info(f"Trovati {len(dataset_pages)} dataset")
        else:
            # La cache usa un dizionario dove le chiavi sono gli URL dei dataset
            dataset_pages = list(cache_data['datasets'].keys())
            logger.info(f"Utilizzo {len(dataset_pages)} dataset dalla cache")
        
        # Chiedi all'utente cosa vuole fare
        print("\nCosa vuoi fare?")
        print("1. Esegui una scansione approfondita di tutti i dataset")
        print("2. Usa la cache esistente per il download")
        print("3. Esci")
        
        choice = input("\nScelta: ").strip()
        
        if choice == "3":
            print("\nOperazione annullata.")
            return
        
        if choice == "1":
            # Mostra progresso
            print("\nAnalisi dettagli dei dataset...")
            
            # Analizziamo tutti i dataset, non solo un campione
            total_json_files = 0
            total_csv_files = 0
            processed_datasets = 0
            skipped_datasets = 0
            errored_datasets = 0
            
            # Usa rich per mostrare una tabella di progresso
            from rich.live import Live
            from rich.table import Table
            
            def generate_stats_table():
                table = Table(title="Stato Scansione Dataset")
                table.add_column("Metriche", justify="left", style="cyan")
                table.add_column("Valore", justify="right", style="green")
                table.add_row("Dataset Processati", str(processed_datasets))
                table.add_row("Dataset Saltati", str(skipped_datasets))
                table.add_row("Dataset con Errori", str(errored_datasets))
                table.add_row("File JSON Trovati", str(total_json_files))
                table.add_row("File CSV Trovati", str(total_csv_files))
                return table
            
            # Processa tutti i dataset con visualizzazione in tempo reale
            with Live(generate_stats_table(), refresh_per_second=4) as live:
                for i, dataset_url in enumerate(dataset_pages):
                    current_dataset_info = {
                        'url': dataset_url,
                        'json_files': [],
                        'csv_files': [],
                        'analyzed': False
                    }
                    
                    # Verifica se il dataset è già nella cache
                    dataset_in_cache = False
                    if cache_data and 'datasets' in cache_data:
                        for cached_dataset in cache_data['datasets']:
                            if cached_dataset['url'] == dataset_url:
                                dataset_in_cache = True
                                current_dataset_info = cached_dataset
                                break
                    
                    if not dataset_in_cache:
                        try:
                            # Recupera i file JSON
                            json_files = await get_json_files(dataset_url)
                            if json_files:
                                current_dataset_info['json_files'] = json_files
                                total_json_files += len(json_files)
                                
                                # Recupera i file CSV
                            csv_files = await get_csv_files(dataset_url)
                            if csv_files:
                                current_dataset_info['csv_files'] = csv_files
                                total_csv_files += len(csv_files)
                                
                                current_dataset_info['analyzed'] = True
                                processed_datasets += 1
                                
                        except Exception as e:
                            logger.logger.error(f"Errore nell'analisi del dataset {dataset_url}: {str(e)}")
                            errored_datasets += 1
                            continue
                    else:
                        skipped_datasets += 1
                    
                    # Aggiorna la cache
                    if cache_data and 'datasets' in cache_data:
                        # Aggiorna o aggiungi il dataset alla cache
                        updated = False
                        for i, cached_dataset in enumerate(cache_data['datasets']):
                            if cached_dataset['url'] == dataset_url:
                                cache_data['datasets'][i] = current_dataset_info
                                updated = True
                                break
                        if not updated:
                            cache_data['datasets'].append(current_dataset_info)
                    else:
                        cache_data = {'datasets': [current_dataset_info]}
                    
                    # Salva la cache periodicamente
                    if (i + 1) % 10 == 0:
                        try:
                            with open(cache_file, 'w', encoding='utf-8') as f:
                                json.dump(cache_data, f, indent=2)
                            logger.logger.info("Cache salvata")
                        except Exception as e:
                            logger.logger.error(f"Errore nel salvataggio della cache: {str(e)}")
                    
                    # Aggiorna la tabella di progresso
                    live.update(generate_stats_table())
            
            # Salva la cache finale
            try:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, indent=2)
                logger.logger.info("Cache finale salvata")
            except Exception as e:
                logger.logger.error(f"Errore nel salvataggio della cache finale: {str(e)}")
            
            # Mostra il menu di download
            print("\nCosa vuoi scaricare?")
            print("1. File JSON")
            print("2. File CSV")
            print("3. Entrambi")
            print("4. Esci")
            
            download_choice = input("\nScelta: ").strip()
            
            if download_choice == "4":
                print("\nOperazione annullata.")
                return
            
            # Inizializza i contatori
            successful_downloads = 0
            failed_downloads = 0
            
            # Conta il numero totale di file da scaricare dalla cache
            total_files = 0
            files_to_download = []
            
            # Debug: stampa la struttura della cache
            print("\nStruttura della cache:")
            for dataset_url, dataset_info in cache_data['datasets'].items():
                print(f"\nDataset: {dataset_url}")
                if 'json_files' in dataset_info:
                    print(f"File JSON: {len(dataset_info['json_files'])}")
                    if download_choice in ["1", "3"]:
                        for file_info in dataset_info['json_files']:
                            total_files += 1
                            files_to_download.append(('json', file_info))
                if 'csv_files' in dataset_info:
                    print(f"File CSV: {len(dataset_info['csv_files'])}")
                    if download_choice in ["2", "3"]:
                        for file_info in dataset_info['csv_files']:
                            total_files += 1
                            files_to_download.append(('csv', file_info))
            
            print(f"\nTrovati {total_files} file da scaricare")
            
            if total_files == 0:
                print("\nNessun file trovato nella cache. Eseguire prima una scansione approfondita.")
                return
            
            # Inizializza lo scraper
            async with ANACScraper() as scraper:
                # Processa i file da scaricare
                for i, (file_type, file_info) in enumerate(files_to_download, 1):
                    print(f"\nFile {i} di {total_files}: {file_info['filename']} ({file_type.upper()})")
                    
                    try:
                        output_dir = os.path.join('downloads', file_type)
                        os.makedirs(output_dir, exist_ok=True)
                        output_file = os.path.join(output_dir, file_info['filename'])
                        
                        # Verifica se il file esiste già e ha la dimensione corretta
                        if file_info['filename'] in existing_json_files:
                            file_data = existing_json_files[file_info['filename']]
                            if await verify_file_integrity(file_data['path'], file_info['size']):
                                logger.info(f"File già esistente e integro: {file_info['filename']}")
                                successful_downloads += 1
                                continue
                            else:
                                logger.info(f"File esistente ma incompleto o corrotto, riprovo il download: {file_info['filename']}")
                                # Rimuovi il file corrotto prima di riprovare il download
                                try:
                                    os.remove(file_data['path'])
                                    logger.info(f"Rimosso file corrotto: {file_info['filename']}")
                                except Exception as e:
                                    logger.error(f"Errore nella rimozione del file corrotto: {str(e)}")
                        
                        # Tenta il download
                        try:
                            if await scraper.download_file(file_info['url'], output_file, file_info['size']):
                                successful_downloads += 1
                                logger.logger.info(f"File scaricato con successo: {file_info['filename']}")
                            else:
                                failed_downloads += 1
                                logger.logger.error(f"Errore nel download del file: {file_info['filename']}")
                        except Exception as e:
                            failed_downloads += 1
                            logger.logger.error(f"Errore nel download del file {file_info['filename']}: {str(e)}")
                            # Se il download fallisce, rimuovi il file incompleto
                            if os.path.exists(output_file):
                                try:
                                    os.remove(output_file)
                                    logger.logger.info(f"Rimosso file incompleto: {output_file}")
                                except:
                                    pass
                    except Exception as e:
                        failed_downloads += 1
                        logger.logger.error(f"Errore nella gestione del file {file_info['filename']}: {str(e)}")
            
            # Mostra il riepilogo finale
            print("\nRiepilogo Download:")
            print(f"Download completati con successo: {successful_downloads}")
            print(f"Download falliti: {failed_downloads}")
        
        elif choice == "2":
            # Usa la cache esistente
            if not cache_data or 'datasets' not in cache_data:
                print("\nNessuna cache disponibile. Eseguire prima una scansione approfondita.")
                return
            
            print("\nUtilizzo della cache esistente per il download...")
            print(f"Dataset nella cache: {len(cache_data['datasets'])}")
            
            # Mostra il menu di download
            print("\nCosa vuoi scaricare?")
            print("1. File JSON")
            print("2. File CSV")
            print("3. Entrambi")
            print("4. Esci")
            
            download_choice = input("\nScelta: ").strip()
            
            if download_choice == "4":
                print("\nOperazione annullata.")
                return
            
            # Inizializza i contatori
            successful_downloads = 0
            failed_downloads = 0
            
            # Conta il numero totale di file da scaricare dalla cache
            total_files = 0
            files_to_download = []
            
            # Debug: stampa la struttura della cache
            print("\nStruttura della cache:")
            for dataset_url, dataset_info in cache_data['datasets'].items():
                print(f"\nDataset: {dataset_url}")
                if 'json_files' in dataset_info:
                    print(f"File JSON: {len(dataset_info['json_files'])}")
                    if download_choice in ["1", "3"]:
                        for file_info in dataset_info['json_files']:
                            total_files += 1
                            files_to_download.append(('json', file_info))
                if 'csv_files' in dataset_info:
                    print(f"File CSV: {len(dataset_info['csv_files'])}")
                    if download_choice in ["2", "3"]:
                        for file_info in dataset_info['csv_files']:
                            total_files += 1
                            files_to_download.append(('csv', file_info))
            
            print(f"\nTrovati {total_files} file da scaricare")
            
            if total_files == 0:
                print("\nNessun file trovato nella cache. Eseguire prima una scansione approfondita.")
                return
            
            # Inizializza lo scraper
            async with ANACScraper() as scraper:
                # Processa i file da scaricare
                for i, (file_type, file_info) in enumerate(files_to_download, 1):
                    print(f"\nFile {i} di {total_files}: {file_info['filename']} ({file_type.upper()})")
                    
                    try:
                        output_dir = os.path.join('downloads', file_type)
                        os.makedirs(output_dir, exist_ok=True)
                        output_file = os.path.join(output_dir, file_info['filename'])
                        
                        # Verifica se il file esiste già e ha la dimensione corretta
                        if file_info['filename'] in existing_json_files:
                            file_data = existing_json_files[file_info['filename']]
                            if await verify_file_integrity(file_data['path'], file_info['size']):
                                logger.info(f"File già esistente e integro: {file_info['filename']}")
                                successful_downloads += 1
                                continue
                            else:
                                logger.info(f"File esistente ma incompleto o corrotto, riprovo il download: {file_info['filename']}")
                                # Rimuovi il file corrotto prima di riprovare il download
                                try:
                                    os.remove(file_data['path'])
                                    logger.info(f"Rimosso file corrotto: {file_info['filename']}")
                                except Exception as e:
                                    logger.error(f"Errore nella rimozione del file corrotto: {str(e)}")
                        
                        # Tenta il download
                        try:
                            if await scraper.download_file(file_info['url'], output_file, file_info['size']):
                                successful_downloads += 1
                                logger.logger.info(f"File scaricato con successo: {file_info['filename']}")
                            else:
                                failed_downloads += 1
                                logger.logger.error(f"Errore nel download del file: {file_info['filename']}")
                        except Exception as e:
                            failed_downloads += 1
                            logger.logger.error(f"Errore nel download del file {file_info['filename']}: {str(e)}")
                            # Se il download fallisce, rimuovi il file incompleto
                            if os.path.exists(output_file):
                                try:
                                    os.remove(output_file)
                                    logger.logger.info(f"Rimosso file incompleto: {output_file}")
                                except:
                                    pass
                    except Exception as e:
                        failed_downloads += 1
                        logger.logger.error(f"Errore nella gestione del file {file_info['filename']}: {str(e)}")
            
            # Mostra il riepilogo finale
            print("\nRiepilogo Download:")
            print(f"Download completati con successo: {successful_downloads}")
            print(f"Download falliti: {failed_downloads}")
        
        else:
            print("\nOperazione annullata.")
            return
    
    except Exception as e:
        # Salva il crash report
        save_crash_report(e, traceback.format_exc(), cache_data)
        raise  # Rilancia l'eccezione per mostrare il traceback completo

if __name__ == "__main__":
    asyncio.run(main()) 