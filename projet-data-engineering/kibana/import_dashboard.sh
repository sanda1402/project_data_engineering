#!/bin/sh
# Attend que Kibana soit prêt puis importe dashboard.ndjson (data views + visualisations + dashboard).
KIBANA_URL="${KIBANA_URL:-http://kibana:5601}"
FILE="${1:-/kibana/dashboard.ndjson}"

echo "Attente de Kibana sur $KIBANA_URL ..."
until curl -s "$KIBANA_URL/api/status" | grep -q '"level":"available"'; do sleep 5; done

echo "Import de $FILE ..."
curl -s -X POST "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" --form file=@"$FILE"
echo
echo "Dashboard disponible : $KIBANA_URL/app/dashboards"
