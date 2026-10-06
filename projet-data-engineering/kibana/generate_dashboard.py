"""Génère kibana/dashboard.ndjson (data views, 8 visualisations, 1 dashboard).

Usage : python kibana/generate_dashboard.py
Import : Kibana > Stack Management > Saved Objects > Import (ou kibana/import_dashboard.sh)
"""
import json
from pathlib import Path

OUT = Path(__file__).with_name("dashboard.ndjson")
VERSION = "8.15.0"
SEARCH = json.dumps({"query": {"query": "", "language": "kuery"}, "filter": [],
                     "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"})


def agg(id_, type_, schema, params=None):
    return {"id": id_, "enabled": True, "type": type_, "params": params or {}, "schema": schema}


def terms(id_, field, schema, order_by="1", size=10, order="desc"):
    return agg(id_, "terms", schema, {"field": field, "orderBy": order_by, "order": order, "size": size,
                                      "otherBucket": False, "otherBucketLabel": "Autres", "missingBucket": False})


def axes(chart_type, y_label):
    return {
        "type": chart_type, "grid": {"categoryLines": False},
        "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom", "show": True,
                          "style": {}, "scale": {"type": "linear"},
                          "labels": {"show": True, "filter": True, "truncate": 100}, "title": {}}],
        "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": "left",
                       "show": True, "style": {}, "scale": {"type": "linear", "mode": "normal"},
                       "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                       "title": {"text": y_label}}],
        "seriesParams": [{"show": True, "type": chart_type, "mode": "normal",
                          "data": {"label": y_label, "id": "1"}, "valueAxis": "ValueAxis-1",
                          "drawLinesBetweenPoints": True, "lineWidth": 2, "showCircles": True}],
        "addTooltip": True, "addLegend": False, "legendPosition": "right", "times": [],
        "addTimeMarker": False, "labels": {"show": False},
        "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full", "color": "#E7664C"},
    }


METRIC_PARAMS = {"addTooltip": True, "addLegend": False, "type": "metric", "metric": {
    "percentageMode": False, "useRanges": False, "colorSchema": "Green to Red", "metricColorMode": "None",
    "colorsRange": [{"from": 0, "to": 10000}], "labels": {"show": True}, "invertColors": False,
    "style": {"bgFill": "#000", "bgColor": False, "labelColor": False, "subText": "", "fontSize": 48}}}
PIE_PARAMS = {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right",
              "isDonut": True, "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}}
TABLE_PARAMS = {"perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False,
                "showTotal": False, "totalFunc": "sum", "percentageCol": ""}

# (id, titre, data view, type, aggs, params)
VISUALS = [
    ("vis-total-products", "Nombre total de produits", "products-dv", "metric",
     [agg("1", "count", "metric")], METRIC_PARAMS),
    ("vis-avg-rating", "Note moyenne des produits", "products-dv", "metric",
     [agg("1", "avg", "metric", {"field": "rating"})], METRIC_PARAMS),
    ("vis-category-pie", "Répartition des produits par catégorie", "products-dv", "pie",
     [agg("1", "count", "metric"), terms("2", "category", "segment", size=15)], PIE_PARAMS),
    ("vis-sentiment-pie", "Sentiment des avis clients", "reviews-dv", "pie",
     [agg("1", "count", "metric"), terms("2", "sentiment", "segment")], PIE_PARAMS),
    ("vis-avg-price", "Prix moyen par catégorie", "products-dv", "histogram",
     [agg("1", "avg", "metric", {"field": "price"}), terms("2", "category", "segment", size=15)],
     axes("histogram", "Prix moyen")),
    ("vis-stock-level", "Produits par niveau de stock", "products-dv", "histogram",
     [agg("1", "count", "metric"), terms("2", "stock_level", "segment")], axes("histogram", "Produits")),
    ("vis-reviews-time", "Évolution du nombre d'avis par mois", "reviews-dv", "line",
     [agg("1", "count", "metric"),
      agg("2", "date_histogram", "segment", {"field": "review_date", "useNormalizedEsInterval": True,
                                             "scaleMetricValues": False, "interval": "M",
                                             "drop_partials": False, "min_doc_count": 1, "extended_bounds": {}})],
     axes("line", "Nombre d'avis")),
    ("vis-top-brands", "Top 10 marques par note moyenne des avis", "products-dv", "table",
     [agg("1", "avg", "metric", {"field": "avg_review_rating"}), agg("3", "count", "metric"),
      terms("2", "brand", "bucket", order_by="1")], TABLE_PARAMS),
]

# (visualisation, x, y, w, h)
LAYOUT = [("vis-total-products", 0, 0, 12, 8), ("vis-avg-rating", 12, 0, 12, 8),
          ("vis-category-pie", 24, 0, 24, 16), ("vis-sentiment-pie", 0, 8, 24, 8),
          ("vis-avg-price", 0, 16, 24, 16), ("vis-stock-level", 24, 16, 24, 16),
          ("vis-reviews-time", 0, 32, 24, 16), ("vis-top-brands", 24, 32, 24, 16)]

objects = []
for dv_id, title, time_field in [("products-dv", "products", None), ("reviews-dv", "reviews", "review_date")]:
    attrs = {"title": title, "name": title}
    if time_field:
        attrs["timeFieldName"] = time_field
    objects.append({"id": dv_id, "type": "index-pattern", "attributes": attrs, "references": []})

for id_, title, dv, type_, aggs, params in VISUALS:
    state = {"title": title, "type": type_, "aggs": aggs, "params": params}
    objects.append({
        "id": id_, "type": "visualization",
        "attributes": {"title": title, "visState": json.dumps(state), "uiStateJSON": "{}", "description": "",
                       "version": 1, "kibanaSavedObjectMeta": {"searchSourceJSON": SEARCH}},
        "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": dv}],
    })

panels, refs = [], []
for i, (vid, x, y, w, h) in enumerate(LAYOUT):
    panels.append({"version": VERSION, "type": "visualization",
                   "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(i)},
                   "panelIndex": str(i), "embeddableConfig": {}, "panelRefName": f"panel_{i}"})
    refs.append({"name": f"panel_{i}", "type": "visualization", "id": vid})

objects.append({
    "id": "dashboard-products-reviews", "type": "dashboard",
    "attributes": {
        "title": "Produits & Avis clients — Dashboard",
        "description": "Indicateurs clés du pipeline ETL Airflow -> Elasticsearch",
        "panelsJSON": json.dumps(panels),
        "optionsJSON": json.dumps({"useMargins": True, "syncColors": False, "hidePanelTitles": False}),
        "timeRestore": True, "timeFrom": "now-5y", "timeTo": "now",
        "refreshInterval": {"pause": True, "value": 0}, "version": 1,
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(
            {"query": {"query": "", "language": "kuery"}, "filter": []})},
    },
    "references": refs,
})

OUT.write_text("\n".join(json.dumps(o, ensure_ascii=False) for o in objects) + "\n", encoding="utf-8")
print(f"{len(objects)} objets écrits dans {OUT}")
