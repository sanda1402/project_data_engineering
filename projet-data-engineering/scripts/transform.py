"""TRANSFORM : nettoyage, enrichissement et jointure produits <-> avis.

Transformations réalisées :
  1. suppression des doublons (sur l'identifiant produit)
  2. gestion des valeurs manquantes (marque, note, stock, remise...)
  3. conversion de types (id, prix, stock...)
  4. normalisation des dates en ISO 8601 UTC
  5. filtrage (prix invalides)
  6. nouvelles colonnes (prix après remise, niveau de stock, gamme de prix, sentiment)
  7. agrégation + jointure (note moyenne et nombre d'avis ajoutés aux produits)
"""
import hashlib
import json
import logging
from datetime import datetime, timezone

import pandas as pd

from config import PROCESSED_DIR, PRODUCTS_FILE, REVIEWS_FILE

log = logging.getLogger(__name__)

RENAME = {
    "discountPercentage": "discount_percentage",
    "availabilityStatus": "availability_status",
    "warrantyInformation": "warranty",
    "shippingInformation": "shipping",
    "returnPolicy": "return_policy",
    "minimumOrderQuantity": "min_order_quantity",
    "meta_createdAt": "created_at",
    "meta_updatedAt": "updated_at",
}

PRODUCT_COLUMNS = [
    "id", "sku", "title", "description", "category", "brand", "price",
    "discount_percentage", "price_after_discount", "rating", "stock",
    "stock_level", "price_range", "availability_status", "tags", "warranty",
    "shipping", "return_policy", "min_order_quantity", "weight", "created_at",
    "updated_at", "nb_reviews", "avg_review_rating", "thumbnail", "ingested_at",
]
REVIEW_COLUMNS = [
    "review_id", "product_id", "product_title", "category", "brand", "rating",
    "sentiment", "comment", "review_date", "reviewer_name", "reviewer_hash",
    "ingested_at",
]


def _iso(series: pd.Series) -> pd.Series:
    """Normalise des dates hétérogènes en ISO 8601 UTC."""
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed.dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _sentiment(rating: int) -> str:
    if rating >= 4:
        return "positif"
    if rating == 3:
        return "neutre"
    return "négatif"


def _build_reviews(df: pd.DataFrame, ingested_at: str) -> pd.DataFrame:
    rows = []
    for _, p in df.iterrows():
        reviews = p.get("reviews")
        if not isinstance(reviews, list):
            continue
        for r in reviews:
            email = str(r.get("reviewerEmail", ""))
            key = f"{p['id']}|{email}|{r.get('date', '')}|{r.get('comment', '')}"
            rating = pd.to_numeric(r.get("rating"), errors="coerce")
            rows.append({
                "review_id": hashlib.sha1(key.encode("utf-8")).hexdigest()[:16],
                "product_id": int(p["id"]),
                "product_title": p["title"],
                "category": p["category"],
                "brand": p["brand"],
                "rating": None if pd.isna(rating) else int(rating),
                "comment": (r.get("comment") or "").strip(),
                "review_date": r.get("date"),
                "reviewer_name": r.get("reviewerName") or "Anonyme",
                # anonymisation : on ne conserve pas l'e-mail
                "reviewer_hash": hashlib.sha1(email.encode("utf-8")).hexdigest()[:12],
                "ingested_at": ingested_at,
            })
    rev = pd.DataFrame(rows)
    if rev.empty:
        return pd.DataFrame(columns=REVIEW_COLUMNS)
    rev["review_date"] = _iso(rev["review_date"])
    rev = rev.drop_duplicates(subset="review_id")                       # doublons d'avis
    rev = rev.dropna(subset=["rating", "review_date"])                   # avis inexploitables
    rev["rating"] = rev["rating"].astype(int)
    rev["sentiment"] = rev["rating"].apply(_sentiment)                   # nouvelle colonne
    return rev[REVIEW_COLUMNS]


def transform_products(raw_path: str) -> dict:
    with open(raw_path, encoding="utf-8") as f:
        raw = json.load(f)
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    df = pd.json_normalize(raw, sep="_").rename(columns=RENAME)
    n_raw = len(df)
    for col in list(RENAME.values()) + ["sku", "description", "thumbnail", "weight",
                                        "brand", "category", "title", "rating",
                                        "stock", "tags", "reviews"]:
        if col not in df.columns:
            df[col] = None

    # 1) Doublons
    df = df.drop_duplicates(subset="id", keep="first").copy()
    n_dupes = n_raw - len(df)

    # 2) Valeurs manquantes
    df["brand"] = df["brand"].fillna("Unknown").astype(str).str.strip()
    df["category"] = df["category"].fillna("unknown").astype(str).str.strip().str.lower()
    df["title"] = df["title"].fillna("Sans titre").astype(str).str.strip()
    df["description"] = df["description"].fillna("")
    df["discount_percentage"] = pd.to_numeric(df["discount_percentage"], errors="coerce").fillna(0.0)
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["rating"] = df["rating"].fillna(df["rating"].median())
    df["availability_status"] = df["availability_status"].fillna("Unknown")
    df["min_order_quantity"] = pd.to_numeric(df["min_order_quantity"], errors="coerce").fillna(1)
    df["tags"] = df["tags"].apply(lambda t: t if isinstance(t, list) else [])

    # 3) Conversion de types
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df.dropna(subset=["id"])
    df["id"] = df["id"].astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["stock"] = df["stock"].astype(int)
    df["min_order_quantity"] = df["min_order_quantity"].astype(int)
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")

    # 4) Dates normalisées
    df["created_at"] = _iso(df["created_at"])
    df["updated_at"] = _iso(df["updated_at"])

    # 5) Filtrage : on écarte les produits sans prix valide
    n_before = len(df)
    df = df[df["price"].notna() & (df["price"] > 0)].copy()
    n_invalid_price = n_before - len(df)

    # 6) Nouvelles colonnes
    df["price_after_discount"] = (df["price"] * (1 - df["discount_percentage"] / 100)).round(2)
    df["stock_level"] = pd.cut(df["stock"], bins=[-1, 0, 10, 50, float("inf")],
                               labels=["rupture", "faible", "moyen", "élevé"]).astype(str)
    df["price_range"] = pd.cut(df["price"], bins=[0, 20, 100, 500, float("inf")],
                               labels=["budget", "standard", "premium", "luxe"]).astype(str)
    df["ingested_at"] = ingested_at

    # 7) Avis + agrégation + jointure
    reviews = _build_reviews(df, ingested_at)
    agg = (reviews.groupby("product_id")["rating"]
           .agg(nb_reviews="count", avg_review_rating="mean").reset_index()
           .rename(columns={"product_id": "id"}))
    agg["avg_review_rating"] = agg["avg_review_rating"].round(2)
    df = df.merge(agg, on="id", how="left")
    df["nb_reviews"] = df["nb_reviews"].fillna(0).astype(int)

    products = df[PRODUCT_COLUMNS]
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    products.to_json(PRODUCTS_FILE, orient="records", lines=True, force_ascii=False)
    reviews.to_json(REVIEWS_FILE, orient="records", lines=True, force_ascii=False)

    stats = {
        "products_path": str(PRODUCTS_FILE),
        "reviews_path": str(REVIEWS_FILE),
        "rows_raw": n_raw,
        "duplicates_removed": n_dupes,
        "invalid_price_removed": n_invalid_price,
        "products_out": len(products),
        "reviews_out": len(reviews),
    }
    log.info("Transformation terminée : %s", stats)
    return stats


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    print(transform_products(sys.argv[1]))
