"""Configuration centrale du projet (chemins, noms d'index, URL de l'API)."""
import os
from pathlib import Path

# Dans Docker : /opt/airflow ; en local : racine du dépôt
BASE_DIR = Path(os.getenv("PROJECT_DIR", Path(__file__).resolve().parents[1]))
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
ES_DIR = BASE_DIR / "elasticsearch"

SAMPLE_FILE = DATA_DIR / "sample_products.json"   # données de secours (offline)
DEFAULT_API_URL = "https://dummyjson.com/products?limit=0"

PRODUCTS_INDEX = "products"
REVIEWS_INDEX = "reviews"

PRODUCTS_FILE = PROCESSED_DIR / "products.jsonl"
REVIEWS_FILE = PROCESSED_DIR / "reviews.jsonl"
QUALITY_REPORT = PROCESSED_DIR / "quality_report.json"
ANALYSIS_FILE = PROCESSED_DIR / "analysis_results.json"
