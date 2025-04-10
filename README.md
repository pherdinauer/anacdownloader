# ANAC Dataset Browser

A simple web application to browse and view ANAC datasets stored in JSON format.

## Features

- Browse available datasets
- View dataset details
- View JSON file contents
- Debug file existence
- Simple and intuitive interface

## Project Structure

```
.
├── backend/                 # Backend Flask application
│   ├── api_server.py       # API endpoints
│   └── models/             # Database models
├── simple-frontend/        # Frontend files
│   ├── app.js             # Frontend JavaScript
│   ├── index.html         # Main HTML file
│   └── styles.css         # CSS styles
├── data/                  # JSON data files
├── config.py              # Configuration settings
├── logger.py              # Logging configuration
├── run_simple.py          # Application entry point
└── requirements.txt       # Python dependencies
```

## Setup

1. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
python run_simple.py
```

4. Open your browser and navigate to:
```
http://localhost:8082
```

## Usage

1. Click "Test Connection" to verify database connectivity
2. Click "Load Datasets" to view available datasets
3. Click on a dataset to view its details and files
4. Click on a file to view its contents
5. Use "Debug Files" to check file existence

## Data Organization

JSON files are organized in the `data/` directory, with each dataset having its own subdirectory:

```
data/
├── dataset1/
│   ├── file1.json
│   └── file2.json
└── dataset2/
    ├── file3.json
    └── file4.json
```

## License

This project is licensed under the MIT License. 