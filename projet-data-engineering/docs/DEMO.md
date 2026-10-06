# Démonstration du projet (≈ 5 minutes) & checklist de dépôt

## A. Script de démonstration (vidéo courte)
| Temps | Action | À dire |
|---|---|---|
| 0:00 | Montrer l'architecture (schéma du README) | « Données produits/avis → Airflow → Elasticsearch → Kibana. » |
| 0:30 | Terminal : `docker compose up -d` puis `docker compose ps` | « Toute la stack est reproductible avec une commande. » |
| 1:00 | Airflow : ouvrir le DAG, vue *Graph* | « 7 tâches, un TaskGroup, planifié @daily, 2 retries. » |
| 1:30 | *Trigger DAG* ; suivre l'exécution en vue *Grid* | « Les tâches passent au vert une à une. » |
| 2:15 | Ouvrir les logs de `transform` puis `validate` | « Doublons supprimés, valeurs manquantes traitées, 21 contrôles qualité OK. » |
| 3:00 | Onglet *XCom* de `transform` | « Les chemins de fichiers et statistiques circulent via XCom. » |
| 3:15 | Terminal : `curl localhost:9200/products/_count` ; `curl localhost:9200/reviews/_count` | « Les documents sont bien indexés. » |
| 3:45 | Kibana : ouvrir le dashboard, filtrer par catégorie | « Prix par catégorie, stock, sentiments, évolution des avis, top marques. » |
| 4:30 | (Optionnel) casser volontairement la qualité : `min_rows` = 1000 dans Admin > Variables, relancer | « Le contrôle échoue, rien n'est chargé : la qualité protège l'index. » |
| 5:00 | Conclusion | « Collecté, transformé, contrôlé, stocké, analysé, visualisé. » |

## B. Captures d'écran à produire (dans `docs/screenshots/`)
- [ ] `airflow_dag_graph.png` — vue Graph du DAG
- [ ] `airflow_dag_grid.png` — exécution réussie (toutes tâches vertes)
- [ ] `kibana_dashboard.png` — dashboard complet avec données

## C. Checklist des livrables
- [ ] Code source complet (`scripts/`)
- [ ] DAG Airflow (`dags/etl_pipeline.py`)
- [ ] Données ou instructions (`data/sample_products.json` + API DummyJSON)
- [ ] Configuration Docker (`Dockerfile`, `docker-compose.yml`, `requirements.txt`)
- [ ] `elasticsearch/mapping.json` et `queries.json`
- [ ] Contrôles qualité (`scripts/validate.py`)
- [ ] `kibana/dashboard.ndjson`
- [ ] README complet avec captures d'écran
- [ ] Courte vidéo de démonstration (lien dans le README ou fichier dans le dépôt)

## D. Publier sur GitHub
```bash
cd projet-data-engineering
git init && git add . && git commit -m "Projet final : pipeline ETL Airflow + Elasticsearch + Kibana"
git branch -M main
git remote add origin https://github.com/<utilisateur>/projet-data-engineering.git
git push -u origin main
```
Avant de rendre : cloner le dépôt dans un dossier vide et suivre le README pour vérifier la reproductibilité.

## E. Questions probables à l'oral
- **Pourquoi deux index ?** Les avis sont une relation 1-N ; un document par avis permet des agrégations temporelles et par sentiment.
- **Pourquoi `dynamic: strict` ?** Pour refuser tout champ inattendu et garder un mapping maîtrisé.
- **Que se passe-t-il si on relance le DAG ?** `_id` déterministe → mise à jour, pas de doublons (idempotence).
- **Pourquoi pas de retry sur l'échec qualité ?** Les données sont en cause, pas l'infrastructure : `AirflowFailException`.
- **Pourquoi XCom ne transporte pas les données ?** Il ne transporte que des chemins/statistiques ; les volumes passent par des fichiers.
