"""EXTRACT : récupère les produits (avec avis) depuis l'API DummyJSON.

Si l'API est inaccessible (ex. pas d'internet), on bascule sur
data/sample_products.json afin que le projet reste reproductible.
"""
import json
import logging
import time
from datetime import datetime, timezone

import requests

from config import DEFAULT_API_URL, RAW_DIR, SAMPLE_FILE

log = logging.getLogger(__name__)


def fetch_from_api(url: str, retries: int = 3, timeout: int = 30) -> list:
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            log.info("Appel API (%s/%s) : %s", attempt, retries, url)
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
            products = payload.get("products", []) if isinstance(payload, dict) else payload
            if not products:
                raise ValueError("Réponse API vide")
            return products
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            log.warning("Tentative %s échouée : %s", attempt, exc)
            time.sleep(2 * attempt)
    raise RuntimeError(f"API injoignable après {retries} tentatives") from last_exc


def extract_products(api_url: str = DEFAULT_API_URL, use_fallback: bool = True,
                     run_date: str | None = None) -> dict:
    """Télécharge les données brutes et les sauvegarde dans data/raw/."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    try:
        products = fetch_from_api(api_url)
        source = "api"
    except Exception as exc:  # noqa: BLE001
        if not use_fallback:
            raise
        log.warning("Bascule sur le fichier d'exemple (%s) : %s", SAMPLE_FILE, exc)
        products = json.loads(SAMPLE_FILE.read_text(encoding="utf-8"))
        source = "sample"

    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_path = RAW_DIR / f"products_{run_date}.json"
    raw_path.write_text(json.dumps(products, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("%s produits extraits (source=%s) -> %s", len(products), source, raw_path)
    return {"raw_path": str(raw_path), "source": source, "nb_records": len(products)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(extract_products())
