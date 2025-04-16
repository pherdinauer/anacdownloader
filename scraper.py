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
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.logging import RichHandler
from rich.table import Table
from rich.panel import Panel
import zipfile
import collections
from rich.live import Live

# Rileva se siamo in un ambiente con terminale limitato
SIMPLE_TERMINAL = 'TMUX' in os.environ or 'SSH_CONNECTION' in os.environ or not sys.stdout.isatty()

# Configurazione console Rich adattiva
if SIMPLE_TERMINAL:
    console = Console(highlight=False, color_system="standard", width=100)
    print("Rilevato ambiente terminale limitato (tmux/ssh). Utilizzo modalità di visualizzazione semplificata.")
else:
    console = Console()

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

class AdvancedLogger:
    def __init__(self, log_file: str = "anac_downloader.log"):
        # Usa la console globale che si adatta all'ambiente
        self.console = console
        self.log_file = log_file
        self.detailed_log_file = "anac_downloader_detailed.log"
        
        # Configura il logging con Rich
        if SIMPLE_TERMINAL:
            # Configurazione più semplice per terminali limitati
            logging.basicConfig(
                level=logging.INFO,
                format="[%(levelname)s] %(asctime)s - %(message)s",
                datefmt="[%X]",
                handlers=[
                    logging.StreamHandler(),
                    logging.FileHandler(log_file, encoding='utf-8')
                ]
            )
        else:
            # Configurazione completa per terminali moderni
            logging.basicConfig(
                level=logging.INFO,
                format="%(message)s",
                datefmt="[%X]",
                handlers=[
                    RichHandler(rich_tracebacks=True, markup=True),
                    logging.FileHandler(log_file, encoding='utf-8')
                ]
            )
        
        self.logger = logging.getLogger("anac_downloader")
        
        # Crea il file di log con intestazione
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"=== ANAC Downloader Log ===\n")
            f.write(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*50 + "\n\n")
        
        # Crea il file di log dettagliato con intestazione
        with open(self.detailed_log_file, 'w', encoding='utf-8') as f:
            f.write(f"=== ANAC Downloader Detailed Log ===\n")
            f.write(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*50 + "\n\n")
            f.write("TIMESTAMP,OPERATION,FILENAME,URL,SIZE,STATUS,DURATION,SPEED,NOTES\n")
        
        # Store download start times
        self.download_start_times = {}
        self.speed_history = {}
        self.download_details = {}
    
    def _format_time(self, seconds: float) -> str:
        """Formatta i secondi in un formato leggibile."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.0f}m {seconds%60:.1f}s"
        else:
            hours = seconds / 3600
            minutes = (seconds % 3600) / 60
            return f"{hours:.0f}h {minutes:.1f}m"
    
    def _calculate_eta(self, filename: str, current: int, total: int, speed: float) -> str:
        """Calcola l'ETA usando la velocità media degli ultimi aggiornamenti."""
        if filename not in self.speed_history:
            self.speed_history[filename] = []
        
        # Add current speed to history (keep last 5)
        self.speed_history[filename].append(speed)
        if len(self.speed_history[filename]) > 5:
            self.speed_history[filename].pop(0)
        
        # Calculate average speed
        avg_speed = sum(self.speed_history[filename]) / len(self.speed_history[filename])
        
        # Calculate ETA
        if avg_speed > 0:
            remaining_bytes = total - current
            eta_seconds = remaining_bytes / avg_speed
            return self._format_time(eta_seconds)
        return "sconosciuto"
    
    def _log_to_detailed_file(self, operation, filename, url="", size=0, status="", duration=0, speed=0, notes=""):
        """Scrive una riga nel file di log dettagliato in formato CSV."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.detailed_log_file, 'a', encoding='utf-8') as f:
            f.write(f"{timestamp},{operation},{filename},{url},{size},{status},{duration},{speed},{notes}\n")
    
    def log_download_start(self, filename: str, size: int, url: str = ""):
        """Log l'inizio di un download"""
        self.download_start_times[filename] = time.time()
        self.speed_history[filename] = []
        self.download_details[filename] = {
            "url": url,
            "size": size,
            "start_time": time.time()
        }
        self.logger.info(f"[bold green]Starting download:[/] {filename} ({self._format_size(size)})")
        self._log_to_detailed_file("START", filename, url, size, "STARTED")
    
    def log_download_progress(self, filename: str, current: int, total: int, speed: float, elapsed: float):
        """Log il progresso di un download con tempo trascorso e ETA"""
        percentage = (current / total) * 100
        
        # Calcola l'ETA usando la velocità media
        eta_str = self._calculate_eta(filename, current, total, speed)
        
        # Formatta il tempo trascorso
        elapsed_str = self._format_time(elapsed)
        
        # Calcola la velocità media dall'inizio
        if elapsed > 0:
            avg_speed = current / elapsed / 1024  # KB/s
        else:
            avg_speed = 0
        
        self.logger.info(
            f"[cyan]Progress:[/] {filename}\n"
            f"├─ Progress: {percentage:.1f}% ({self._format_size(current)}/{self._format_size(total)})\n"
            f"├─ Current Speed: {speed/1024:.1f} KB/s (Avg: {avg_speed:.1f} KB/s)\n"
            f"├─ Elapsed: {elapsed_str}\n"
            f"└─ ETA: {eta_str}"
        )
    
    def log_download_complete(self, filename: str, duration: float):
        """Log il completamento di un download"""
        duration_str = self._format_time(duration)
        
        # Recupera i dettagli del download
        details = self.download_details.get(filename, {})
        url = details.get("url", "")
        size = details.get("size", 0)
        avg_speed = size / duration / 1024 if duration > 0 else 0  # KB/s
        
        # Clean up tracking data
        if filename in self.download_start_times:
            del self.download_start_times[filename]
        if filename in self.speed_history:
            del self.speed_history[filename]
        if filename in self.download_details:
            del self.download_details[filename]
        
        self.logger.info(f"[bold green]✓ Download completed:[/] {filename} in {duration_str}")
        self._log_to_detailed_file("COMPLETE", filename, url, size, "COMPLETED", duration, avg_speed)
    
    def log_download_error(self, filename: str, error: str, url: str = ""):
        """Log un errore di download"""
        # Recupera i dettagli del download
        details = self.download_details.get(filename, {})
        size = details.get("size", 0)
        duration = time.time() - self.download_start_times.get(filename, time.time())
        
        # Clean up tracking data
        if filename in self.download_start_times:
            del self.download_start_times[filename]
        if filename in self.speed_history:
            del self.speed_history[filename]
        if filename in self.download_details:
            del self.download_details[filename]
        
        self.logger.error(f"[bold red]✗ Download failed:[/] {filename} - {error}")
        self._log_to_detailed_file("ERROR", filename, url, size, f"FAILED: {error}", duration)
    
    def log_chunk_status(self, filename: str, chunk_num: int, total_chunks: int, status: str):
        """Log lo stato di un chunk"""
        # Calculate elapsed time for this file if available
        elapsed_str = ""
        if filename in self.download_start_times:
            elapsed = time.time() - self.download_start_times[filename]
            elapsed_str = f" (elapsed: {self._format_time(elapsed)})"
        
        self.logger.info(
            f"[yellow]Chunk {chunk_num}/{total_chunks}[/] for {filename}: {status}{elapsed_str}"
        )
        self._log_to_detailed_file("CHUNK", filename, "", 0, f"Chunk {chunk_num}/{total_chunks}: {status}")
    
    def log_retry(self, filename: str, attempt: int, max_attempts: int, delay: float):
        """Log un tentativo di retry"""
        # Calculate elapsed time for this file if available
        elapsed_str = ""
        if filename in self.download_start_times:
            elapsed = time.time() - self.download_start_times[filename]
            elapsed_str = f" (elapsed: {self._format_time(elapsed)})"
        
        self.logger.warning(
            f"[yellow]Retry {attempt}/{max_attempts}[/] for {filename} "
            f"after {delay:.1f}s delay{elapsed_str}"
        )
        self._log_to_detailed_file("RETRY", filename, "", 0, f"Retry {attempt}/{max_attempts} after {delay:.1f}s")
    
    def _format_size(self, size_bytes: int) -> str:
        """Formatta i byte in un formato leggibile"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"
    
    def log_file_info(self, filename: str, url: str, size: int, file_type: str, dataset_url: str = ""):
        """Log informazioni dettagliate su un file trovato"""
        self.logger.info(f"[blue]File info:[/] {filename} ({self._format_size(size)}) - {file_type}")
        self._log_to_detailed_file("INFO", filename, url, size, "FOUND", 0, 0, f"Type: {file_type}, Dataset: {dataset_url}")

    def warning(self, message: str):
        """Log un messaggio di warning."""
        self.logger.warning(message)

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
            
            # Prepara il comando curl senza limitazioni di velocità
            cmd = [
                "curl",
                "--retry", "2",              # Solo 2 retry
                "--retry-delay", "5",        # Attesa breve tra i retry
                "--connect-timeout", "15",   # Timeout connessione ridotto
                "--max-time", "1800",        # 30 minuti massimo
                "--compressed",              # Compressione
                "--location",                # Segui redirect
                "--silent",                  # Modalità silenziosa
                "--show-error",              # Mostra errori
                "-H", "User-Agent: Mozilla/5.0",
                "-H", "Accept: */*",
                "-H", "Connection: keep-alive",
                url,
                "--output", output_file
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0 and os.path.exists(output_file):
                actual_size = os.path.getsize(output_file)
                if actual_size >= file_size * 0.99:  # Permette una differenza dell'1%
                    self.logger.logger.info(f"Download veloce completato: {os.path.basename(output_file)}")
                    return True
                else:
                    self.logger.logger.warning(f"Dimensione file non corrispondente. Prevista: {file_size}, Attuale: {actual_size}")
            else:
                error_msg = stderr.decode() if stderr else "Errore sconosciuto"
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
            
            # Dimensione di ogni segmento (1MB)
            segment_size = 1 * 1024 * 1024
            
            # Nome file temporaneo per il download parziale
            temp_file = output_file + ".part"
            
            # Verifica se esiste già un file parziale
            start_position = 0
            if os.path.exists(temp_file):
                start_position = os.path.getsize(temp_file)
                self.logger.info(f"Trovato download parziale: {start_position/(1024*1024):.1f}MB, riprendo da lì")
            
            # Calcola quanti segmenti devono essere scaricati
            total_segments = math.ceil(file_size / segment_size)
            start_segment = start_position // segment_size
            
            self.logger.info(f"Download segmentato per {os.path.basename(output_file)} ({file_size/(1024*1024):.1f}MB) - {total_segments} segmenti totali")
            
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
                    segment_start = segment_idx * segment_size
                    segment_end = min(segment_start + segment_size - 1, file_size - 1)
                    
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
                                    if attempt < 4:  # Riprova se non è l'ultimo tentativo
                                        await asyncio.sleep(5 * (attempt + 1))
                                        continue
                                    return False
                                
                                # Leggi i dati di questo segmento
                                data = await response.read()
                                expected_size = segment_end - segment_start + 1
                                
                                if len(data) != expected_size:
                                    self.logger.warning(f"Dimensioni segmento non corrispondono: previsto {expected_size}, ricevuto {len(data)}")
                                    if attempt < 4:  # Riprova se non è l'ultimo tentativo
                                        await asyncio.sleep(5 * (attempt + 1))
                                        continue
                                
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
                                
                                # Successo per questo segmento, interrompi il ciclo di tentativi
                                break
                                
                        except asyncio.TimeoutError:
                            self.logger.warning(f"Timeout durante il download del segmento {segment_idx+1}")
                            if attempt < 4:  # Riprova se non è l'ultimo tentativo
                                await asyncio.sleep(5 * (attempt + 1))
                                continue
                            return False
                        except Exception as e:
                            self.logger.error(f"Errore durante il download del segmento {segment_idx+1}: {str(e)}")
                            if attempt < 4:  # Riprova se non è l'ultimo tentativo
                                await asyncio.sleep(5 * (attempt + 1))
                                continue
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

    async def _download_segment_with_curl(self, url, segment_file, start_byte, end_byte, segment_idx, limit_rate):
        """Download a segment using curl with specific byte range."""
        try:
            cmd = [
                "curl", "-L", "-o", segment_file,
                "--retry", "10",
                "--retry-delay", "15",
                "--retry-max-time", "600",
                "--connect-timeout", "60",
                "--speed-time", "60",
                "--speed-limit", "500",
                "--limit-rate", f"{limit_rate}k",
                "-H", f"User-Agent: {self.headers['User-Agent']}",
                "-H", f"Referer: {self.headers['Referer']}",
                "-H", f"Range: bytes={start_byte}-{end_byte}",
                "-H", "Accept: */*",
                "-H", "Connection: keep-alive",
            ]
            
            # Add cookie file if available
            cookie_file = None
            if self.cookies:
                try:
                    cookie_file = f".curl_cookies_seg{segment_idx}.txt"
                    with open(cookie_file, 'w') as f:
                        for name, value in self.cookies.items():
                            f.write(f"dati.anticorruzione.it\tTRUE\t/\tFALSE\t0\t{name}\t{value}\n")
                    cmd.extend(["--cookie", cookie_file])
                except Exception as e:
                    self.logger.error(f"Errore cookie per segmento {segment_idx}: {str(e)}")
            
            # Append or create mode
            if os.path.exists(segment_file) and os.path.getsize(segment_file) > 0:
                cmd.append("-C")
                cmd.append("-")
            
            cmd.append(url)
            
            # Run curl
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Wait for completion with timeout (30 min)
            return_code = await asyncio.wait_for(process.wait(), timeout=1800)
            
            # Clean up cookie file
            if cookie_file and os.path.exists(cookie_file):
                try:
                    os.remove(cookie_file)
                except:
                    pass
            
            if return_code == 0:
                # Verify segment size
                if os.path.exists(segment_file):
                    actual_size = os.path.getsize(segment_file)
                    expected_size = end_byte - start_byte + 1
                    
                    if abs(actual_size - expected_size) <= 1024:  # Allow 1KB difference
                        print(f"Segmento {segment_idx+1}: Completato ({actual_size/(1024*1024):.1f}MB)")
                        return True
            
            print(f"Segmento {segment_idx+1}: Errore curl ({return_code})")
            return False
            
        except Exception as e:
            print(f"Segmento {segment_idx+1}: Errore curl - {str(e)}")
            return False
            
    async def download_with_multi_connection(self, url, output_path, file_size):
        """Download a file using multiple connections in parallel (IDM-style)."""
        filename = os.path.basename(output_path)
        temp_dir = os.path.join(os.path.dirname(output_path), ".temp")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Use the configured number of connections
        num_connections = self.download_options.get('num_connections', 1)  # Default to 1
        print(f"Utilizzo {num_connections} connessioni per il download")
        
        # Smaller segment size for better resumability
        segment_size = self.download_options.get('segment_size', 5 * 1024 * 1024)  # 5MB per segmento
        
        # Determine chunk size - each connection downloads a specific part
        chunk_size = file_size // num_connections
        if chunk_size < segment_size:  # Ensure minimum segment size
            chunk_size = segment_size
            num_connections = min(num_connections, file_size // chunk_size)
            if num_connections < 1:
                num_connections = 1
            print(f"Ridimensionato a {num_connections} connessioni (min {segment_size/(1024*1024):.1f}MB per segmento)")
        
        # Create segment file paths
        segment_files = []
        for i in range(num_connections):
            segment_files.append(os.path.join(temp_dir, f"{filename}.part{i}"))
        
        # Define ranges for each segment
        ranges = []
        for i in range(num_connections):
            start = i * chunk_size
            end = (i+1) * chunk_size - 1 if i < num_connections - 1 else file_size - 1
            ranges.append((start, end))
        
        # Check if segments exist and get their size
        for i, segment_file in enumerate(segment_files):
            if os.path.exists(segment_file):
                size = os.path.getsize(segment_file)
                if size > 0:
                    # If segment exists, adjust start position
                    ranges[i] = (ranges[i][0] + size, ranges[i][1])
                    print(f"Segmento {i+1}: Riprendo da {size} bytes")
                    
        # Nuova strategia: scarica un segmento alla volta per minimizzare gli errori
        if num_connections == 1 or file_size < 20 * 1024 * 1024:  # Per file piccoli o se impostato a 1 connessione
            print("Utilizzo strategia conservativa: un segmento alla volta")
            for i, (start_byte, end_byte) in enumerate(ranges):
                segment_file = segment_files[i]
                success = await self._download_segment_conservative(url, segment_file, start_byte, end_byte, i)
                if not success:
                    print(f"Download del segmento {i+1} fallito dopo tutti i tentativi")
                    return False
            
            # Se arriviamo qui, tutti i segmenti sono stati scaricati con successo
            success = True
        else:
            # Download function for a segment
            async def download_segment(session, segment_idx, start_byte, end_byte, segment_file):
                if start_byte > end_byte:
                    print(f"Segmento {segment_idx+1} già completato")
                    return True
                
                headers = self.headers.copy()
                headers['Range'] = f'bytes={start_byte}-{end_byte}'
                
                mode = 'ab' if os.path.exists(segment_file) else 'wb'
                retries = 0
                max_retries = 15  # More retries for segments
                
                while retries < max_retries:
                    try:
                        print(f"Segmento {segment_idx+1}: Download {start_byte}-{end_byte} ({(end_byte-start_byte+1)/(1024*1024):.1f}MB)")
                        
                        # Wait between retries with increasing delay
                        if retries > 0:
                            wait_time = min(30 * retries, 600)  # Max 10 minutes wait
                            print(f"Segmento {segment_idx+1}: Attendo {wait_time}s prima di riprovare (tentativo {retries+1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                        
                        # Longer random delay to avoid simultaneous requests
                        await asyncio.sleep(random.uniform(5, 15))
                        
                        # Set a lower timeout for individual segments
                        timeout = aiohttp.ClientTimeout(total=1800)  # 30 minutes per segment
                        
                        # Lower rate limit for individual segments
                        limit_rate = self.download_options.get('limit_rate', 200) // num_connections
                        
                        # Use curl for segment download if available
                        if self.curl_available and start_byte < end_byte:
                            return await self._download_segment_with_curl(url, segment_file, start_byte, end_byte, segment_idx, limit_rate)
                        
                        # Fall back to aiohttp if curl not available
                        async with self.session.get(url, headers=headers, timeout=timeout) as response:
                            if response.status not in (200, 206):
                                print(f"Segmento {segment_idx+1}: Errore HTTP {response.status}")
                                retries += 1
                                continue
                            
                            with open(segment_file, mode) as f:
                                downloaded = 0
                                async for chunk in response.content.iter_chunked(8192):
                                    if not chunk:
                                        break
                                    f.write(chunk)
                                    downloaded += len(chunk)
                                    # Periodically update progress
                                    if downloaded % (512*1024) == 0:  # Update every 512KB
                                        current = start_byte + downloaded
                                        percent = (current - ranges[segment_idx][0]) / (end_byte - ranges[segment_idx][0] + 1) * 100
                                        print(f"Segmento {segment_idx+1}: {percent:.1f}% completato")
                        
                        # Verify segment size
                        if os.path.exists(segment_file):
                            actual_size = os.path.getsize(segment_file)
                            expected_size = (end_byte - ranges[segment_idx][0] + 1) + (start_byte - ranges[segment_idx][0])
                            
                            if abs(actual_size - expected_size) <= 1024:  # Allow 1KB difference
                                print(f"Segmento {segment_idx+1}: Completato ({actual_size/(1024*1024):.1f}MB)")
                                return True
                            else:
                                print(f"Segmento {segment_idx+1}: Dimensione non corrisponde. Attesa: {expected_size}, Reale: {actual_size}")
                                # If we downloaded something, adjust start position and continue
                                if actual_size > 0:
                                    start_byte = ranges[segment_idx][0] + actual_size
                                    headers['Range'] = f'bytes={start_byte}-{end_byte}'
                                    mode = 'ab'  # Append mode
                        
                        retries += 1
                    
                    except asyncio.TimeoutError:
                        print(f"Segmento {segment_idx+1}: Timeout")
                        retries += 1
                        # Adjust range if partial data was downloaded
                        if os.path.exists(segment_file):
                            actual_size = os.path.getsize(segment_file)
                            if actual_size > 0:
                                start_byte = ranges[segment_idx][0] + actual_size
                                headers['Range'] = f'bytes={start_byte}-{end_byte}'
                                mode = 'ab'  # Append mode
                    
                    except Exception as e:
                        print(f"Segmento {segment_idx+1}: Errore - {str(e)}")
                        retries += 1
                
                print(f"Segmento {segment_idx+1}: Fallito dopo {max_retries} tentativi")
                return False
            
            # Start downloading all segments in parallel
            tasks = []
            for i in range(num_connections):
                start_byte, end_byte = ranges[i]
                task = asyncio.create_task(
                    download_segment(self.session, i, start_byte, end_byte, segment_files[i])
                )
                tasks.append(task)
            
            # Wait for all segments to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Check if all segments completed successfully
            success = True
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"Segmento {i+1}: Eccezione - {str(result)}")
                    success = False
                elif not result:
                    print(f"Segmento {i+1}: Download fallito")
                    success = False
        
        if success:
            # All segments downloaded successfully, merge them
            try:
                print(f"Tutti i segmenti completati. Unione in corso...")
                with open(output_path, 'wb') as outfile:
                    for segment_file in segment_files:
                        if os.path.exists(segment_file):
                            with open(segment_file, 'rb') as infile:
                                while True:
                                    data = infile.read(8192)
                                    if not data:
                                        break
                                    outfile.write(data)
                
                # Verify final file size
                if os.path.exists(output_path):
                    final_size = os.path.getsize(output_path)
                    if abs(final_size - file_size) <= 1024:  # Allow 1KB difference
                        print(f"✓ File completato: {filename} ({final_size/(1024*1024):.1f}MB)")
                        
                        # Cleanup segment files
                        for segment_file in segment_files:
                            try:
                                if os.path.exists(segment_file):
                                    os.remove(segment_file)
                            except:
                                pass
                        
                        # Update download state
                        if filename in self.download_state['failed_files']:
                            self.download_state['failed_files'].remove(filename)
                        if filename in self.download_state['partial_downloads']:
                            del self.download_state['partial_downloads'][filename]
                        if filename not in self.download_state['completed_files']:
                            self.download_state['completed_files'].append(filename)
                        self.save_state()
                        
                        return True
                    else:
                        print(f"✗ Dimensione file finale non corrisponde: {final_size} vs {file_size}")
            except Exception as e:
                print(f"Errore durante l'unione dei segmenti: {str(e)}")
        
        # If we get here, something failed. Keep segment files for resuming later.
        print(f"Download multi-connessione non completato. I segmenti sono stati conservati per la ripresa.")
        
        # Update partial download state
        self.download_state['partial_downloads'][filename] = {
            'url': url,
            'path': output_path,
            'segments': [
                {
                    'file': segment_file,
                    'range': ranges[i]
                } for i, segment_file in enumerate(segment_files) if os.path.exists(segment_file)
            ],
            'total_bytes': file_size,
            'last_attempt': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.save_state()
        
        return False
        
    async def _download_segment_conservative(self, url, segment_file, start_byte, end_byte, segment_idx):
        """Download a segment using a more conservative approach with smaller chunks."""
        print(f"Avvio download conservativo per segmento {segment_idx+1}")
        
        # Chunk size for conservative download (1MB chunks)
        chunk_size = 1 * 1024 * 1024  # 1MB
        
        # Ensure segment directory exists
        os.makedirs(os.path.dirname(segment_file), exist_ok=True)
        
        # Initialize file if it doesn't exist
        if not os.path.exists(segment_file):
            with open(segment_file, 'wb') as f:
                pass
        
        # Get current file size
        current_size = os.path.getsize(segment_file)
        if current_size > 0:
            start_byte += current_size
        
        # Check if already complete
        if start_byte >= end_byte:
            print(f"Segmento {segment_idx+1} già completo")
            return True
        
        # Calculate number of chunks
        total_bytes = end_byte - start_byte + 1
        num_chunks = math.ceil(total_bytes / chunk_size)
        
        print(f"Segmento {segment_idx+1} diviso in {num_chunks} mini-chunks da {chunk_size/1024/1024:.1f}MB")
        
        # Start download
        current_byte = start_byte
        mode = 'ab'  # Always append
        
        for chunk_idx in range(num_chunks):
            chunk_start = current_byte
            chunk_end = min(chunk_start + chunk_size - 1, end_byte)
            
            print(f"Segmento {segment_idx+1}, Chunk {chunk_idx+1}/{num_chunks}: Scaricando bytes {chunk_start}-{chunk_end}")
            
            # Try up to 5 times for each chunk
            for attempt in range(5):
                try:
                    # Add delay between chunks and attempts
                    if attempt > 0:
                        delay = 10 * (2 ** attempt)  # Exponential backoff
                        print(f"Attesa di {delay}s prima del tentativo {attempt+1}/5")
                        await asyncio.sleep(delay)
                    else:
                        # Random delay between chunks
                        delay = random.uniform(5, 15)
                        print(f"Attesa di {delay:.1f}s tra chunks")
                        await asyncio.sleep(delay)
                    
                    # Prepare headers with range
                    headers = self.headers.copy()
                    headers['Range'] = f'bytes={chunk_start}-{chunk_end}'
                    
                    # Rate limit
                    limit_rate = self.download_options.get('limit_rate', 200)
                    
                    # Use curl for each chunk if available
                    if self.curl_available:
                        success = await self._download_chunk_with_curl(url, segment_file, chunk_start, chunk_end, segment_idx, chunk_idx, limit_rate, mode)
                        if success:
                            current_byte = chunk_end + 1
                            break  # Success, move to next chunk
                        elif attempt == 4:  # Last attempt failed
                            print(f"Chunk {chunk_idx+1} fallito dopo 5 tentativi")
                            return False
                    else:
                        # Use aiohttp fallback
                        timeout = aiohttp.ClientTimeout(total=600)  # 10 minutes per chunk
                        
                        async with self.session.get(url, headers=headers, timeout=timeout) as response:
                            if response.status not in (200, 206):
                                print(f"Errore HTTP {response.status} per chunk {chunk_idx+1}")
                                if attempt == 4:  # Last attempt
                                    return False
                                continue  # Try again
                            
                            data = await response.read()
                            
                            # Verify data size
                            expected_size = chunk_end - chunk_start + 1
                            if len(data) != expected_size:
                                print(f"Dimensione data non corretta: atteso {expected_size}, ricevuto {len(data)}")
                                if attempt == 4:  # Last attempt
                                    return False
                                continue  # Try again
                            
                            # Write data
                            with open(segment_file, mode) as f:
                                f.seek(current_byte - start_byte)  # Position in file
                                f.write(data)
                            
                            current_byte = chunk_end + 1
                            break  # Success, move to next chunk
                except Exception as e:
                    print(f"Errore durante download chunk {chunk_idx+1}: {str(e)}")
                    if attempt == 4:  # Last attempt
                        return False
            
            # Update progress after each chunk
            progress = (current_byte - start_byte) / (end_byte - start_byte + 1) * 100
            print(f"Segmento {segment_idx+1}: {progress:.1f}% completato ({(current_byte-start_byte)/(1024*1024):.1f}MB/{total_bytes/(1024*1024):.1f}MB)")
        
        # Verify final segment size
        if os.path.exists(segment_file):
            final_size = os.path.getsize(segment_file)
            expected_size = end_byte - start_byte + 1 + current_size  # Include existing data
            
            if abs(final_size - expected_size) <= 1024:  # Allow 1KB difference
                print(f"✓ Segmento {segment_idx+1} completato: {final_size/(1024*1024):.1f}MB")
                return True
            else:
                print(f"✗ Dimensione segmento non corrisponde: atteso {expected_size/(1024*1024):.1f}MB, ottenuto {final_size/(1024*1024):.1f}MB")
                return False
        
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
                    self.logger.logger.info(f"File {os.path.basename(output_file)} esiste ma dimensione diversa (attuale: {current_size}, attesa: {file_size}). Ridownload.")
            
            # Assicurati che la directory esista
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Preparazione per il download
            if self.session is None:
                self.session = aiohttp.ClientSession(headers=self.headers, timeout=self.timeout)
            
            # Registra l'inizio del download
            start_time = time.time()
            filename = os.path.basename(output_file)
            self.logger.log_download_start(filename, file_size, url)
            
            # Controlla se possiamo riprendere il download
            mode = 'ab'
            start_byte = 0
            if os.path.exists(output_file):
                start_byte = os.path.getsize(output_file)
                self.logger.logger.info(f"Riprendo download da {start_byte/(1024*1024):.1f}MB")
            else:
                mode = 'wb'
            
            # Prepara gli header
            headers = self.headers.copy()
            if start_byte > 0:
                headers['Range'] = f'bytes={start_byte}-'
            
            # Effettua la richiesta
            async with self.session.get(url, headers=headers, timeout=self.timeout) as response:
                if response.status not in (200, 206):
                    self.logger.logger.error(f"Errore HTTP: {response.status}")
                    return False
                
                # Avvia il monitoraggio del progresso
                monitor_task = asyncio.create_task(
                    self._monitor_download_progress_with_eta(output_file, file_size)
                )
                
                # Scarica a blocchi
                with open(output_file, mode) as f:
                    downloaded_size = start_byte
                    async for chunk in response.content.iter_chunked(self.chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                
                # Cancella il task di monitoraggio
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    pass
                
                # Verifica dimensione finale
                if os.path.exists(output_file):
                    actual_size = os.path.getsize(output_file)
                    if abs(actual_size - file_size) <= 1024:  # Tollera differenza dell'1%
                        elapsed = time.time() - start_time
                        self.logger.log_download_complete(filename, elapsed)
                        return True
                    else:
                        self.logger.log_download_error(
                            filename, 
                            f"Dimensione non corrispondente: attesa {file_size}, reale {actual_size}",
                            url
                        )
                
                return False
                
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
            
            # Rimuovi duplicati mantenendo il primo URL per ogni nome file
            seen_filenames = set()
            unique_json_files = []
            for file_info in json_files:
                filename = file_info['filename']
                # Verifica ulteriore che non sia un file invalido
                if (filename and 
                    not any(pattern in filename.lower() for pattern in ['#', 'mailto', 'accessibilit']) and
                    (filename.lower().endswith('.json') or filename.lower().endswith('.zip'))):
                    
                    if filename not in seen_filenames:
                        seen_filenames.add(filename)
                        unique_json_files.append(file_info)
            
            self.logger.logger.info(f"Trovati {len(unique_json_files)} file JSON/ZIP unici")
            
            # Aggiungi dettagli sui file trovati per debug
            if unique_json_files:
                for file_info in unique_json_files:
                    self.logger.logger.info(f"File trovato: {file_info['filename']} ({self._format_size(file_info['size'])})")
            else:
                self.logger.logger.warning(f"Nessun file trovato in {dataset_url}")
                
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

async def main():
    """Funzione principale per il download completo di tutti i dataset."""
    # Crea le directory per i download
    output_dir = "downloads"
    os.makedirs(os.path.join(output_dir, "json"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "csv"), exist_ok=True)
    
    # File JSON di appoggio per i dataset e i relativi file
    datasets_cache_file = "datasets_cache.json"
    
    print("\nAnalizziamo il sito dati.anticorruzione.it per trovare tutti i dataset disponibili...")
    
    try:
        async with ANACScraper() as scraper:
            cache_data = {}
            
            # Controlla se esiste già un file cache
            if os.path.exists(datasets_cache_file):
                try:
                    with open(datasets_cache_file, 'r') as f:
                        cache_data = json.load(f)
                    print(f"\nUtilizzo cache esistente con {len(cache_data.get('datasets', {}))} dataset")
                    
                    # Validità della cache: se è più vecchia di 7 giorni, la consideriamo scaduta
                    if 'last_updated' in cache_data:
                        last_updated = datetime.fromisoformat(cache_data['last_updated'])
                        days_old = (datetime.now() - last_updated).days
                        if days_old > 7:
                            print(f"La cache è vecchia di {days_old} giorni. Aggiornamento in corso...")
                            cache_data = {}  # Reset della cache
                except Exception as e:
                    print(f"Errore nella lettura della cache: {str(e)}. Creazione di una nuova cache.")
                    cache_data = {}
            
            # Se non abbiamo una cache valida, recupera la lista completa dei dataset
            if not cache_data or 'datasets' not in cache_data:
                print("\nRecupero lista completa dei dataset...")
                dataset_pages = await scraper.get_dataset_pages()
                total_datasets = len(dataset_pages)
                print(f"\n✓ Trovati {total_datasets} dataset totali")
                
                # Inizializza la cache
                cache_data = {
                    'last_updated': datetime.now().isoformat(),
                    'total_datasets': total_datasets,
                    'datasets': {}
                }
                
                # Mostra progresso
                print("\nAnalisi dettagli dei dataset...")
                
                # Analizziamo tutti i dataset, non solo un campione
                total_json_files = 0
                total_csv_files = 0
                
                # Counter per file trovati in tempo reale
                real_json_counter = 0
                real_csv_counter = 0
                processed_datasets = 0
                skipped_datasets = 0
                error_datasets = 0
                
                # Crea una tabella per i contatori in tempo reale
                from rich.live import Live
                from rich.table import Table
                
                def generate_stats_table():
                    table = Table(title="Stato Scansione Dataset")
                    table.add_column("Metriche", justify="left", style="cyan")
                    table.add_column("Valore", justify="right", style="green")
                    
                    table.add_row("Dataset Processati", f"{processed_datasets}/{total_datasets} ({processed_datasets/total_datasets*100:.1f}%)")
                    table.add_row("Dataset Saltati", str(skipped_datasets))
                    table.add_row("Dataset con Errori", str(error_datasets))
                    table.add_row("File JSON Trovati", str(real_json_counter))
                    table.add_row("File CSV Trovati", str(real_csv_counter))
                    table.add_row("Totale File", str(real_json_counter + real_csv_counter))
                    
                    if processed_datasets > 0:
                        table.add_row("Media JSON per Dataset", f"{real_json_counter/processed_datasets:.2f}")
                    
                    return table
                
                # Se siamo in un terminale limitato, usa l'output semplice
                if SIMPLE_TERMINAL:
                    # Processa tutti i dataset con output semplice
                    for i, dataset_url in enumerate(dataset_pages):
                        current_dataset_info = {
                            'url': dataset_url,
                            'json_files': [],
                            'csv_files': [],
                            'analyzed': False,
                            'error': None,
                            'processing_time': 0
                        }
                        
                        # Verifica se questo dataset è già stato analizzato e salvato nella cache
                        if dataset_url in cache_data['datasets'] and cache_data['datasets'][dataset_url].get('analyzed', False):
                            processed_datasets += 1
                            skipped_datasets += 1
                            real_json_counter += len(cache_data['datasets'][dataset_url].get('json_files', []))
                            real_csv_counter += len(cache_data['datasets'][dataset_url].get('csv_files', []))
                            print(f"Dataset {processed_datasets}/{total_datasets} ({dataset_url}): saltato (già in cache)")
                            continue
                        
                        start_time = time.time()
                        
                        try:
                            # Recupera i file JSON
                            json_files = await scraper.get_json_files(dataset_url)
                            current_dataset_info['json_files'] = json_files
                            real_json_counter += len(json_files)
                            
                            # Recupera i file CSV
                            csv_files = await scraper.get_csv_files(dataset_url)
                            current_dataset_info['csv_files'] = csv_files
                            real_csv_counter += len(csv_files)
                            
                            current_dataset_info['analyzed'] = True
                            processed_datasets += 1
                            processing_time = time.time() - start_time
                            
                            print(f"Dataset {processed_datasets}/{total_datasets} ({processed_datasets/total_datasets*100:.1f}%): "
                                  f"{len(json_files)} JSON, {len(csv_files)} CSV - {processing_time:.1f}s")
                            
                        except Exception as e:
                            current_dataset_info['error'] = str(e)
                            error_datasets += 1
                            processed_datasets += 1
                            scraper.logger.logger.error(f"Errore nell'analisi del dataset {dataset_url}: {str(e)}")
                            print(f"Dataset {processed_datasets}/{total_datasets}: ERRORE - {str(e)}")
                        
                        # Calcola il tempo di elaborazione
                        current_dataset_info['processing_time'] = time.time() - start_time
                        
                        # Aggiungiamo il dataset alla cache
                        cache_data['datasets'][dataset_url] = current_dataset_info
                        
                        # Salviamo periodicamente la cache
                        if i % 10 == 0 or i == total_datasets - 1:
                            cache_data['last_updated'] = datetime.now().isoformat()
                            cache_data['total_json_files'] = real_json_counter
                            cache_data['total_csv_files'] = real_csv_counter
                            
                            with open(datasets_cache_file, 'w') as f:
                                json.dump(cache_data, f, indent=2)
                else:
                    # Processa tutti i dataset con visualizzazione in tempo reale
                    with Live(generate_stats_table(), refresh_per_second=4) as live:
                        for i, dataset_url in enumerate(dataset_pages):
                            current_dataset_info = {
                                'url': dataset_url,
                                'json_files': [],
                                'csv_files': [],
                                'analyzed': False,
                                'error': None,
                                'processing_time': 0
                            }
                            
                            # Verifica se questo dataset è già stato analizzato e salvato nella cache
                            if dataset_url in cache_data['datasets'] and cache_data['datasets'][dataset_url].get('analyzed', False):
                                processed_datasets += 1
                                skipped_datasets += 1
                                real_json_counter += len(cache_data['datasets'][dataset_url].get('json_files', []))
                                real_csv_counter += len(cache_data['datasets'][dataset_url].get('csv_files', []))
                                live.update(generate_stats_table())
                                continue
                            
                            start_time = time.time()
                            
                            try:
                                # Recupera i file JSON
                                json_files = await scraper.get_json_files(dataset_url)
                                current_dataset_info['json_files'] = json_files
                                real_json_counter += len(json_files)
                                
                                # Recupera i file CSV
                                csv_files = await scraper.get_csv_files(dataset_url)
                                current_dataset_info['csv_files'] = csv_files
                                real_csv_counter += len(csv_files)
                                
                                current_dataset_info['analyzed'] = True
                                processed_datasets += 1
                                
                            except Exception as e:
                                current_dataset_info['error'] = str(e)
                                error_datasets += 1
                                processed_datasets += 1
                                scraper.logger.logger.error(f"Errore nell'analisi del dataset {dataset_url}: {str(e)}")
                            
                            # Calcola il tempo di elaborazione
                            current_dataset_info['processing_time'] = time.time() - start_time
                            
                            # Aggiungiamo il dataset alla cache
                            cache_data['datasets'][dataset_url] = current_dataset_info
                            
                            # Aggiorna la visualizzazione
                            live.update(generate_stats_table())
                            
                            # Salviamo periodicamente la cache
                            if i % 10 == 0 or i == total_datasets - 1:
                                cache_data['last_updated'] = datetime.now().isoformat()
                                cache_data['total_json_files'] = real_json_counter
                                cache_data['total_csv_files'] = real_csv_counter
                                
                                with open(datasets_cache_file, 'w') as f:
                                    json.dump(cache_data, f, indent=2)
                
                # Salva la cache finale
                cache_data['last_updated'] = datetime.now().isoformat()
                cache_data['total_json_files'] = real_json_counter
                cache_data['total_csv_files'] = real_csv_counter
                
                with open(datasets_cache_file, 'w') as f:
                    json.dump(cache_data, f, indent=2)
                
                print("\n✓ Analisi completa terminata e salvata nel file cache")
                
                # Rapporto dettagliato
                print(f"\nRAPPORTO FINALE:")
                print(f"✓ Dataset analizzati: {processed_datasets}/{total_datasets}")
                print(f"✓ Dataset saltati (già in cache): {skipped_datasets}")
                print(f"✓ Dataset con errori: {error_datasets}")
                print(f"✓ File JSON effettivi trovati: {real_json_counter}")
                print(f"✓ File CSV effettivi trovati: {real_csv_counter}")
                print(f"✓ Totale file trovati: {real_json_counter + real_csv_counter}")
                
                if processed_datasets > 0:
                    print(f"✓ Media file JSON per dataset: {real_json_counter/processed_datasets:.2f}")
                    print(f"✓ Media file CSV per dataset: {real_csv_counter/processed_datasets:.2f}")
                
                # Identifica i dataset con più file
                top_json_datasets = sorted(
                    [d for d in cache_data['datasets'].values() if d.get('analyzed', False)],
                    key=lambda x: len(x.get('json_files', [])), 
                    reverse=True
                )[:5]
                
                if top_json_datasets:
                    print("\nTop 5 dataset con più file JSON:")
                    for i, dataset in enumerate(top_json_datasets, 1):
                        print(f"{i}. {dataset['url']} - {len(dataset.get('json_files', []))} file JSON")
                
            # A questo punto abbiamo una cache aggiornata con tutti i dataset
            dataset_pages = list(cache_data.get('datasets', {}).keys())
            total_datasets = len(dataset_pages)
            
            # Conteggio file dai dati in cache
            total_json_files = cache_data.get('total_json_files', 0)
            if total_json_files == 0:
                total_json_files = sum(len(dataset_info.get('json_files', [])) for dataset_info in cache_data.get('datasets', {}).values() if dataset_info.get('analyzed', False))
            
            total_csv_files = cache_data.get('total_csv_files', 0)
            if total_csv_files == 0:
                total_csv_files = sum(len(dataset_info.get('csv_files', [])) for dataset_info in cache_data.get('datasets', {}).values() if dataset_info.get('analyzed', False))
            
            # Menu interattivo
            print("\n=== ANAC Dataset Downloader ===")
            print(f"Dataset totali: {total_datasets}")
            print(f"File JSON trovati: {total_json_files}")
            print(f"File CSV trovati: {total_csv_files}")
            print("-------------------------")
            print("1. Scarica file JSON")
            print("2. Scarica file CSV")
            print("3. Scarica entrambi")
            print("4. Riprendi download interrotti")
            print("5. Mostra statistiche")
            print("6. Scarica file specifico")
            print("7. Rigenera cache")
            print("8. Verifica integrità cache")
            print("9. Esci")
            
            choice = input("\nScegli un'opzione (1-9): ")
            
            if choice == "9":
                return
            
            # Opzione per verificare l'integrità della cache
            if choice == "8":
                print("\nVerifica dell'integrità della cache in corso...")
                
                total_expected = len(cache_data.get('datasets', {}))
                analyzed_count = sum(1 for info in cache_data.get('datasets', {}).values() if info.get('analyzed', False))
                error_count = sum(1 for info in cache_data.get('datasets', {}).values() if info.get('error'))
                missing_json = sum(1 for info in cache_data.get('datasets', {}).values() if info.get('analyzed', False) and not info.get('json_files'))
                missing_csv = sum(1 for info in cache_data.get('datasets', {}).values() if info.get('analyzed', False) and not info.get('csv_files'))
                
                print(f"Dataset totali nella cache: {total_expected}")
                print(f"Dataset analizzati: {analyzed_count} ({analyzed_count/total_expected*100:.1f}%)")
                print(f"Dataset con errori: {error_count} ({error_count/total_expected*100:.1f}%)")
                print(f"Dataset senza file JSON: {missing_json} ({missing_json/analyzed_count*100:.1f}% degli analizzati)")
                print(f"Dataset senza file CSV: {missing_csv} ({missing_csv/analyzed_count*100:.1f}% degli analizzati)")
                
                # Verifica URL duplicati o mancanti
                all_json_urls = []
                for info in cache_data.get('datasets', {}).values():
                    if not info.get('analyzed', False):
                        continue
                    for file_info in info.get('json_files', []):
                        all_json_urls.append(file_info.get('url'))
                
                duplicate_urls = {url: count for url, count in collections.Counter(all_json_urls).items() if count > 1}
                
                if duplicate_urls:
                    print(f"\nTrovati {len(duplicate_urls)} URL duplicati nei file JSON")
                    for url, count in list(duplicate_urls.items())[:5]:  # Mostra solo i primi 5
                        print(f"- {url}: trovato {count} volte")
                    if len(duplicate_urls) > 5:
                        print(f"...e altri {len(duplicate_urls) - 5} URL duplicati")
                
                print("\nVerifica completata")
                input("\nPremi invio per tornare al menu principale...")
                return
                
            # Opzione per rigenerare la cache
            if choice == "7":
                print("\nRigenerazione della cache in corso...")
                # Elimina il file cache esistente
                if os.path.exists(datasets_cache_file):
                    os.remove(datasets_cache_file)
                print("Cache eliminata. Riavvia lo script per generare una nuova cache.")
                return
                
            # Opzione per scaricare un file specifico
            if choice == "6":
                print("\n=== Download File Specifico ===")
                dataset_url = input("Inserisci l'URL del dataset (es. /opendata/dataset/ocds-appalti-ordinari-2022): ")
                if not dataset_url.startswith('/'):
                    dataset_url = f"/opendata/dataset/{dataset_url}"
                
                file_type = input("Tipo di file (json/csv): ").lower()
                if file_type not in ["json", "csv"]:
                    print("Tipo file non valido. Deve essere 'json' o 'csv'.")
                    return
                
                print(f"\nRecupero file {file_type.upper()} dal dataset {dataset_url}...")
                
                try:
                    # Verifica se il dataset è nella cache
                    if dataset_url in cache_data['datasets'] and cache_data['datasets'][dataset_url]['analyzed']:
                        files = cache_data['datasets'][dataset_url][f'{file_type}_files']
                        print(f"Utilizzando dati dalla cache per {dataset_url}")
                    else:
                        # Se non è nella cache, recupera i dati direttamente
                        if file_type == "json":
                            files = await scraper.get_json_files(dataset_url)
                        else:
                            files = await scraper.get_csv_files(dataset_url)
                    
                    if not files:
                        print(f"Nessun file {file_type.upper()} trovato in questo dataset.")
                        return
                    
                    print(f"\nTrovati {len(files)} file {file_type.upper()}:")
                    for i, file_info in enumerate(files, 1):
                        print(f"{i}. {file_info['filename']}")
                    
                    file_choice = input("\nScegli il numero del file da scaricare (0 per tutti): ")
                    
                    if file_choice == "0":
                        # Scarica tutti i file
                        for file_info in files:
                            file_size = await scraper.get_file_size(file_info['url'])
                            print(f"Dimensione file: {file_size/1024/1024:.1f}MB")
                            
                            success = await scraper.download_file(
                                file_info['url'],
                                os.path.join(output_dir, file_type, file_info['filename']),
                                file_size
                            )
                            
                            if success:
                                print(f"✓ Download completato: {file_info['filename']}")
                            else:
                                print(f"✗ Download fallito: {file_info['filename']}")
                    else:
                        try:
                            idx = int(file_choice) - 1
                            if 0 <= idx < len(files):
                                file_info = files[idx]
                                file_size = await scraper.get_file_size(file_info['url'])
                                print(f"Dimensione file: {file_size/1024/1024:.1f}MB")
                                
                                # Opzioni avanzate per il download
                                limit_rate = input("Limite velocità (KB/s, default: 500): ")
                                limit_rate = int(limit_rate) if limit_rate.isdigit() else 500
                                
                                retries = input("Numero di tentativi (default: 5): ")
                                retries = int(retries) if retries.isdigit() else 5
                                
                                print(f"Avvio download di {file_info['filename']} con limite {limit_rate}KB/s e {retries} tentativi...")
                                
                                # Imposta le opzioni personalizzate
                                scraper.download_options = {
                                    'limit_rate': limit_rate,
                                    'retries': retries,
                                }
                                
                                success = await scraper.download_file(
                                    file_info['url'],
                                    os.path.join(output_dir, file_type, file_info['filename']),
                                    file_size
                                )
                                
                                if success:
                                    print(f"✓ Download completato: {file_info['filename']}")
                                else:
                                    print(f"✗ Download fallito: {file_info['filename']}")
                            else:
                                print("Scelta non valida.")
                        except (ValueError, IndexError):
                            print("Scelta non valida.")
                except Exception as e:
                    print(f"Errore durante il recupero dei file: {str(e)}")
                
                return
            
            # Opzione per mostrate statistiche dettagliate
            if choice == "5":
                print("\n=== Statistiche Dettagliate ===")
                print(f"Dataset totali: {total_datasets}")
                print(f"File JSON totali: {total_json_files}")
                print(f"File CSV totali: {total_csv_files}")
                
                # Dataset con più file JSON
                json_leader = max(
                    ((url, len(info['json_files'])) for url, info in cache_data['datasets'].items() if info['analyzed'] and info['json_files']),
                    key=lambda x: x[1],
                    default=('Nessuno', 0)
                )
                print(f"\nDataset con più file JSON: {json_leader[0]} ({json_leader[1]} file)")
                
                # Dataset con più file CSV
                csv_leader = max(
                    ((url, len(info['csv_files'])) for url, info in cache_data['datasets'].items() if info['analyzed'] and info['csv_files']),
                    key=lambda x: x[1],
                    default=('Nessuno', 0)
                )
                print(f"Dataset con più file CSV: {csv_leader[0]} ({csv_leader[1]} file)")
                
                # Media file per dataset
                json_avg = total_json_files / total_datasets if total_datasets > 0 else 0
                csv_avg = total_csv_files / total_datasets if total_datasets > 0 else 0
                print(f"\nMedia file JSON per dataset: {json_avg:.2f}")
                print(f"Media file CSV per dataset: {csv_avg:.2f}")
                
                # Statistiche sui dataset con errori
                error_datasets = sum(1 for info in cache_data['datasets'].values() if info.get('error'))
                print(f"\nDataset con errori: {error_datasets} ({error_datasets/total_datasets*100:.1f}%)")
                
                input("\nPremi invio per tornare al menu principale...")
                return
            
            # Riutilizziamo la lista di dataset già recuperata
            if choice in ["1", "2", "3"]:
                # Statistiche
                successful_downloads = 0
                failed_downloads = 0
                
                for index, dataset_url in enumerate(dataset_pages, 1):
                    print(f"\nProcessando dataset {index}/{total_datasets}: {dataset_url}")
                    
                    # Verifichiamo se abbiamo i dati nella cache
                    if dataset_url in cache_data['datasets'] and cache_data['datasets'][dataset_url]['analyzed']:
                        dataset_info = cache_data['datasets'][dataset_url]
                        
                        # Gestione file JSON
                        if choice in ["1", "3"] and dataset_info['json_files']:
                            json_files = dataset_info['json_files']
                            print(f"Trovati {len(json_files)} file JSON nella cache")
                            
                            for file_info in json_files:
                                try:
                                    file_size = await scraper.get_file_size(file_info['url'])
                                    success = await scraper.download_file(
                                        file_info['url'],
                                        os.path.join(output_dir, "json", file_info['filename']),
                                        file_size
                                    )
                                    if success:
                                        successful_downloads += 1
                                        print(f"✓ Download completato: {file_info['filename']}")
                                    else:
                                        failed_downloads += 1
                                        print(f"✗ Download fallito: {file_info['filename']}")
                                except Exception as e:
                                    failed_downloads += 1
                                    print(f"✗ Errore durante il download di {file_info['filename']}: {str(e)}")
                                    scraper.logger.logger.error(f"Errore nel download di {file_info['filename']}: {str(e)}")
                        
                        # Gestione file CSV
                        if choice in ["2", "3"] and dataset_info['csv_files']:
                            csv_files = dataset_info['csv_files']
                            print(f"Trovati {len(csv_files)} file CSV nella cache")
                            
                            for file_info in csv_files:
                                try:
                                    file_size = await scraper.get_file_size(file_info['url'])
                                    success = await scraper.download_file(
                                        file_info['url'],
                                        os.path.join(output_dir, "csv", file_info['filename']),
                                        file_size
                                    )
                                    if success:
                                        successful_downloads += 1
                                        print(f"✓ Download completato: {file_info['filename']}")
                                    else:
                                        failed_downloads += 1
                                        print(f"✗ Download fallito: {file_info['filename']}")
                                except Exception as e:
                                    failed_downloads += 1
                                    print(f"✗ Errore durante il download di {file_info['filename']}: {str(e)}")
                                    scraper.logger.logger.error(f"Errore nel download di {file_info['filename']}: {str(e)}")
                    else:
                        # Se non abbiamo i dati nella cache, li recuperiamo al momento
                        print(f"Dataset non presente nella cache, recupero dati in tempo reale...")
                        
                        # Gestione file JSON
                        if choice in ["1", "3"]:
                            try:
                                json_files = await scraper.get_json_files(dataset_url)
                                print(f"Trovati {len(json_files)} file JSON")
                                
                                for file_info in json_files:
                                    try:
                                        file_size = await scraper.get_file_size(file_info['url'])
                                        success = await scraper.download_file(
                                            file_info['url'],
                                            os.path.join(output_dir, "json", file_info['filename']),
                                            file_size
                                        )
                                        if success:
                                            successful_downloads += 1
                                            print(f"✓ Download completato: {file_info['filename']}")
                                        else:
                                            failed_downloads += 1
                                            print(f"✗ Download fallito: {file_info['filename']}")
                                    except Exception as e:
                                        failed_downloads += 1
                                        print(f"✗ Errore durante il download di {file_info['filename']}: {str(e)}")
                                        scraper.logger.logger.error(f"Errore nel download di {file_info['filename']}: {str(e)}")
                            except Exception as e:
                                print(f"✗ Errore nel recupero dei file JSON: {str(e)}")
                                scraper.logger.logger.error(f"Errore nel recupero dei file JSON da {dataset_url}: {str(e)}")
                        
                        # Gestione file CSV
                        if choice in ["2", "3"]:
                            try:
                                csv_files = await scraper.get_csv_files(dataset_url)
                                print(f"Trovati {len(csv_files)} file CSV")
                                
                                for file_info in csv_files:
                                    try:
                                        file_size = await scraper.get_file_size(file_info['url'])
                                        success = await scraper.download_file(
                                            file_info['url'],
                                            os.path.join(output_dir, "csv", file_info['filename']),
                                            file_size
                                        )
                                        if success:
                                            successful_downloads += 1
                                            print(f"✓ Download completato: {file_info['filename']}")
                                        else:
                                            failed_downloads += 1
                                            print(f"✗ Download fallito: {file_info['filename']}")
                                    except Exception as e:
                                        failed_downloads += 1
                                        print(f"✗ Errore durante il download di {file_info['filename']}: {str(e)}")
                                        scraper.logger.logger.error(f"Errore nel download di {file_info['filename']}: {str(e)}")
                            except Exception as e:
                                print(f"✗ Errore nel recupero dei file CSV: {str(e)}")
                                scraper.logger.logger.error(f"Errore nel recupero dei file CSV da {dataset_url}: {str(e)}")
                
                print("\n=== Riepilogo ===")
                print(f"Download completati: {successful_downloads}")
                print(f"Download falliti: {failed_downloads}")
                print(f"Totale: {successful_downloads + failed_downloads}")
                return
            
            if choice == "1":
                # Scarica tutti i file JSON
                print("\nScaricamento file JSON in corso...")
                download_dir = os.path.join(output_dir, "json")
                total_json_to_download = total_json_files
                json_downloaded = 0
                json_skipped = 0
                json_errors = 0
                
                # Crea una tabella per lo stato del download
                def generate_download_table():
                    table = Table(title="Stato Download File JSON")
                    table.add_column("Metriche", justify="left", style="cyan")
                    table.add_column("Valore", justify="right", style="green")
                    
                    table.add_row("File Scaricati", f"{json_downloaded}/{total_json_to_download} ({json_downloaded/total_json_to_download*100:.1f}% completato)")
                    table.add_row("File Saltati (già esistenti)", str(json_skipped))
                    table.add_row("Errori", str(json_errors))
                    table.add_row("File Rimanenti", str(total_json_to_download - json_downloaded - json_skipped))
                    
                    if json_downloaded > 0:
                        progress = json_downloaded / total_json_to_download
                        table.add_row("Barra Progresso", f"[{'=' * int(progress * 40)}{' ' * (40 - int(progress * 40))}] {progress*100:.1f}%")
                    
                    return table
                
                # Processa tutti i dataset con visualizzazione in tempo reale
                with Live(generate_download_table(), refresh_per_second=2) as live:
                    # Iteriamo su tutti i dataset nella cache
                    for dataset_url, dataset_info in cache_data['datasets'].items():
                        if not dataset_info.get('analyzed', False):
                            continue
                        
                        # Estrai i file JSON da questo dataset
                        for file_info in dataset_info.get('json_files', []):
                            file_url = file_info.get('url', '')
                            if not file_url:
                                continue
                            
                            # Genera un nome file basato sull'URL
                            filename = os.path.basename(file_url)
                            if not filename:
                                # Se il nome file non può essere estratto, usa l'hash dell'URL
                                filename = f"file_{hashlib.md5(file_url.encode()).hexdigest()}.json"
                                
                            # Se l'URL non termina con .json o .zip, aggiungi l'estensione
                            if not (filename.lower().endswith('.json') or filename.lower().endswith('.zip')):
                                filename += '.json'
                                
                            output_path = os.path.join(download_dir, filename)
                            
                            # Verifica se il file esiste già
                            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                                json_skipped += 1
                                live.update(generate_download_table())
                                continue
                                
                            try:
                                # Ottieni la dimensione del file
                                file_size = await scraper.get_file_size(file_url)
                                
                                # Scarica il file
                                success = await scraper.download_file(file_url, output_path, file_size)
                                
                                if success:
                                    # Processa il file scaricato (estrai se è uno ZIP)
                                    await scraper.process_downloaded_file(output_path)
                                    json_downloaded += 1
                                else:
                                    json_errors += 1
                                    
                            except Exception as e:
                                json_errors += 1
                                scraper.logger.logger.error(f"Errore nel download del file {file_url}: {str(e)}")
                                
                            # Aggiorna la visualizzazione
                            live.update(generate_download_table())
                
                print(f"\n✓ Download completato: {json_downloaded} file scaricati, {json_skipped} saltati, {json_errors} errori")
                return
    
    except KeyboardInterrupt:
        print("\n\nOperazione interrotta dall'utente. I download parziali verranno salvati e potranno essere ripresi in seguito.")
        print("Esci dal programma e riavvialo selezionando l'opzione 4 per riprendere i download interrotti.")
    except asyncio.CancelledError:
        print("\n\nOperazione cancellata. I downloads parziali verranno salvati e potranno essere ripresi in seguito.")
        print("Esci dal programma e riavvialo selezionando l'opzione 4 per riprendere i download interrotti.")
    except Exception as e:
        print(f"\n\nErrore imprevisto: {str(e)}")
        import traceback
        traceback.print_exc()
        print("\nI downloads parziali sono stati salvati e potranno essere ripresi in seguito.")

if __name__ == "__main__":
    asyncio.run(main()) 