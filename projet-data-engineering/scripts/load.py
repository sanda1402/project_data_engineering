"""LOAD : création des index Elasticsearch (avec mapping) et chargement en bulk."""
import json
import logging

from elasticsearch import Elasticsearch, helpers

from config import ES_DIR, PRODUCTS_INDEX, REVIEWS_INDEX

log = logging.getLogger(__name__)


def get_client(es_url: str) -> Elasticsearch:
    return Elasticsearch(es_url, request_timeout=30)


def check_connection(es_url: str) -> dict:
    es = get_client(es_url)
    if not es.ping():
        raise ConnectionError(f"Elasticsearch injoignable : {es_url}")
    info = es.info()
    log.info("Connecté à Elasticsearch %s", info["version"]["number"])
    return {"cluster": info["cluster_name"], "version": info["version"]["number"]}


def create_indices(es_url: str, recreate: bool = False) -> dict:
    """Crée les index à partir de elasticsearch/mapping.json."""
    es = get_client(es_url)
    mappings = json.loads((ES_DIR / "mapping.json").read_text(encoding="utf-8"))
    created = {}
    for index, definition in mappings.items():
        exists = es.indices.exists(index=index)
        if exists and recreate:
            es.indices.delete(index=index)
            exists = False
        if not exists:
            es.indices.create(index=index, settings=definition.get("settings"),
                              mappings=definition["mappings"])
            log.info("Index '%s' créé", index)
        else:
            log.info("Index '%s' déjà présent (conservé)", index)
        created[index] = not exists
    return created


def _actions(path: str, index: str, id_field: str):
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                doc = json.loads(line)
                # _id déterministe => rechargement idempotent (pas de doublons)
                yield {"_index": index, "_id": doc[id_field], "_source": doc}


def load_documents(es_url: str, products_path: str, reviews_path: str) -> dict:
    es = get_client(es_url)
    ok_p, _ = helpers.bulk(es, _actions(products_path, PRODUCTS_INDEX, "id"), chunk_size=500)
    ok_r, _ = helpers.bulk(es, _actions(reviews_path, REVIEWS_INDEX, "review_id"), chunk_size=500)
    es.indices.refresh(index=f"{PRODUCTS_INDEX},{REVIEWS_INDEX}")
    counts = {
        "products_indexed": ok_p,
        "reviews_indexed": ok_r,
        "products_in_index": es.count(index=PRODUCTS_INDEX)["count"],
        "reviews_in_index": es.count(index=REVIEWS_INDEX)["count"],
    }
    log.info("Chargement terminé : %s", counts)
    return counts
