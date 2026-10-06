"""ANALYSE : exécute les requêtes de elasticsearch/queries.json et sauvegarde les résultats."""
import json
import logging

from config import ANALYSIS_FILE, ES_DIR, PROCESSED_DIR
from load import get_client

log = logging.getLogger(__name__)


def _summarize(resp: dict) -> dict:
    out = {"total_hits": resp["hits"]["total"]["value"]}
    if resp.get("aggregations"):
        out["aggregations"] = resp["aggregations"]
    if resp["hits"]["hits"]:
        out["hits"] = [h["_source"] for h in resp["hits"]["hits"]]
    return out


def run_analyses(es_url: str) -> dict:
    es = get_client(es_url)
    queries = json.loads((ES_DIR / "queries.json").read_text(encoding="utf-8"))
    results = {}
    for q in queries:
        resp = es.search(index=q["index"], **q["body"])
        results[q["name"]] = {"question": q["question"], **_summarize(resp.body)}
        log.info("ANALYSE %s — %s\n%s", q["name"], q["question"],
                 json.dumps(results[q["name"]].get("aggregations", results[q["name"]].get("hits")),
                            ensure_ascii=False, indent=2)[:1500])
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    ANALYSIS_FILE.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"analyses_executed": len(results), "output": str(ANALYSIS_FILE)}
