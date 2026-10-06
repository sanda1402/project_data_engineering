"""VALIDATE : contrôles de qualité sur les données transformées.

Contrôles : nombre de lignes, champs obligatoires, unicité des identifiants,
types, bornes de valeurs, intégrité référentielle avis -> produits.
Le rapport est sauvegardé dans data/processed/quality_report.json.
"""
import json
import logging
from datetime import datetime, timezone

import pandas as pd

from config import PROCESSED_DIR, QUALITY_REPORT

log = logging.getLogger(__name__)


class DataQualityError(ValueError):
    """Levée lorsqu'au moins un contrôle qualité échoue."""


def _read_jsonl(path: str) -> pd.DataFrame:
    with open(path, encoding="utf-8") as f:
        return pd.DataFrame([json.loads(line) for line in f if line.strip()])


def _check(name: str, passed: bool, details: str = "") -> dict:
    log.info("[%s] %s %s", "OK" if passed else "KO", name, details)
    return {"check": name, "passed": bool(passed), "details": details}


def validate_data(products_path: str, reviews_path: str, min_rows: int = 10) -> dict:
    products = _read_jsonl(products_path)
    reviews = _read_jsonl(reviews_path)
    checks = []

    # --- Produits ---
    checks.append(_check("products: nombre de lignes >= min_rows",
                         len(products) >= min_rows, f"{len(products)} lignes (min {min_rows})"))
    required_p = ["id", "title", "category", "brand", "price", "rating", "stock"]
    for col in required_p:
        nulls = int(products[col].isna().sum()) if col in products else len(products)
        checks.append(_check(f"products: '{col}' obligatoire", nulls == 0, f"{nulls} valeur(s) nulle(s)"))
    checks.append(_check("products: id unique", products["id"].is_unique,
                         f"{int(products['id'].duplicated().sum())} doublon(s)"))
    checks.append(_check("products: price numérique et > 0",
                         pd.api.types.is_numeric_dtype(products["price"]) and (products["price"] > 0).all()))
    checks.append(_check("products: rating dans [0, 5]", products["rating"].between(0, 5).all()))
    checks.append(_check("products: stock entier >= 0",
                         pd.api.types.is_integer_dtype(products["stock"]) and (products["stock"] >= 0).all()))
    checks.append(_check("products: prix après remise <= prix",
                         (products["price_after_discount"] <= products["price"] + 1e-9).all()))

    # --- Avis ---
    checks.append(_check("reviews: au moins un avis", len(reviews) > 0, f"{len(reviews)} avis"))
    if len(reviews) > 0:
        for col in ["review_id", "product_id", "rating", "review_date"]:
            nulls = int(reviews[col].isna().sum())
            checks.append(_check(f"reviews: '{col}' obligatoire", nulls == 0, f"{nulls} valeur(s) nulle(s)"))
        checks.append(_check("reviews: review_id unique", reviews["review_id"].is_unique))
        checks.append(_check("reviews: rating dans [1, 5]", reviews["rating"].between(1, 5).all()))
        orphans = int((~reviews["product_id"].isin(products["id"])).sum())
        checks.append(_check("reviews: product_id existe dans products", orphans == 0, f"{orphans} orphelin(s)"))

    failed = [c for c in checks if not c["passed"]]
    report = {
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(checks),
        "failed_checks": len(failed),
        "status": "FAILED" if failed else "PASSED",
        "checks": checks,
    }
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    QUALITY_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if failed:
        raise DataQualityError("Contrôles échoués : " + "; ".join(c["check"] for c in failed))
    log.info("Qualité des données : %s/%s contrôles réussis", len(checks), len(checks))
    return {"status": report["status"], "total_checks": len(checks), "failed_checks": 0}


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    print(validate_data(sys.argv[1], sys.argv[2]))
