import os
from pathlib import Path

# Directory di base del progetto
BASE_DIR = Path(__file__).resolve().parent

# Directory per i download
DOWNLOAD_DIR = BASE_DIR / 'data'

# Directory per i log
LOG_DIR = BASE_DIR / 'logs'

# Crea le directory se non esistono
DOWNLOAD_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Configurazione logging
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '[%(levelname)s] %(asctime)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': LOG_DIR / 'downloader.log',
            'formatter': 'standard',
            'encoding': 'utf-8',
        },
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': True
        },
    }
}

# ANAC website base URL
BASE_URL = "https://dati.anticorruzione.it"

# Request settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
REQUEST_TIMEOUT = 30  # seconds

# Supported file types
SUPPORTED_TYPES = ['json', 'csv', 'zip']

# Test mode (if True, only download first dataset)
TEST_MODE = False

# Base URLs
DATASET_URL = f"{BASE_URL}/dataset"
DOWNLOAD_URL = f"{BASE_URL}/download/dataset"

# File types
FILE_TYPES = {
    'json': '.json',
    'csv': '.csv',
    'zip': '.zip'
} 