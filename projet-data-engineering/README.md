# Pipeline ETL — Produits & Avis clients
**Apache Airflow · Elasticsearch · Kibana · Docker**

Projet final de Data Engineering — *Collecter → Transformer → Contrôler → Charger → Analyser → Visualiser*

> **Auteur :** _Votre nom_ · **Date de dépôt :** 06/10/2026

---

## 1. Contexte
Une entreprise e-commerce veut suivre son catalogue produits et la satisfaction de ses clients. Ce projet automatise la collecte quotidienne des produits et de leurs avis depuis une API publique, nettoie et contrôle les données, les charge dans Elasticsearch et les expose dans un dashboard Kibana.

**Domaine choisi :** produits et avis clients.
**Questions métier :** quelles catégories sont les plus chères ? Quelles marques sont les mieux notées ? Quels produits sont à réapprovisionner ? Les clients sont-ils satisfaits, et comment cela évolue-t-il ?

## 2. Architecture

```
 API DummyJSON ──► Airflow (@daily)
                    │
   check_elasticsearch ► extract ► transform ► validate ► create_indices ► load_documents ► analyze
                                                                                │
                                                                       Elasticsearch ──► Kibana
```

| Service | Rôle | URL |
|---|---|---|
| Airflow (webserver + scheduler) | Orchestration | http://localhost:8080 (admin / admin) |
| Elasticsearch 8.15 | Stockage & analyse | http://localhost:9200 |
| Kibana 8.15 | Visualisation | http://localhost:5601 |
| PostgreSQL | Base de métadonnées Airflow | interne |

## 3. Source de données
API publique **DummyJSON** : `https://dummyjson.com/products?limit=0` (≈ 194 produits, chacun avec ses avis clients, sans clé API).
Si l'API est injoignable, le pipeline bascule automatiquement sur `data/sample_products.json` (32 enregistrements, avec défauts volontaires : doublon, marque manquante, note manquante, prix négatif) afin de rester reproductible hors-ligne.

## 4. Structure du dépôt
```
projet-data-engineering/
├── dags/etl_pipeline.py          # DAG Airflow
├── scripts/
│   ├── config.py                 # chemins, noms d'index
│   ├── extract.py                # collecte API (+ fallback)
│   ├── transform.py              # nettoyage / enrichissement
│   ├── validate.py               # contrôles qualité
│   ├── load.py                   # index ES + bulk
│   └── analyze.py                # exécution des requêtes d'analyse
├── data/                         # raw/, processed/, sample_products.json
├── elasticsearch/{mapping.json, queries.json}
├── kibana/{dashboard.ndjson, generate_dashboard.py, import_dashboard.sh}
├── docs/screenshots/             # captures d'écran (Airflow, Kibana)
├── Dockerfile · docker-compose.yml · requirements.txt · README.md
```

## 5. Le DAG Airflow : `etl_products_reviews`

| Fonctionnalité | Mise en œuvre |
|---|---|
| Planification | `schedule="@daily"`, `catchup=False` |
| Dépendances | 7 tâches enchaînées + 1 `TaskGroup` (`elasticsearch_load`) |
| Opérateur | `PythonOperator` |
| Connection | `elasticsearch_default` (variable d'env. `AIRFLOW_CONN_ELASTICSEARCH_DEFAULT`) |
| Variables | `products_api_url`, `min_rows`, `use_fallback` |
| Retries | `retries=2`, `retry_delay=1 min` (pas de retry sur un échec qualité : `AirflowFailException`) |
| Logs | `logging` dans chaque script, visibles dans l'onglet *Logs* de chaque tâche |
| XCom | chemins de fichiers et statistiques passés entre `extract → transform → validate → load` |
| Callback | `on_failure_callback` qui journalise l'échec |
| Contrôle qualité | tâche `validate` bloquante |

## 6. Transformations (`scripts/transform.py`)
| # | Transformation | Détail |
|---|---|---|
| 1 | Suppression des doublons | sur `id` produit et `review_id` |
| 2 | Valeurs manquantes | marque → `Unknown`, note → médiane, stock/remise → 0, catégorie normalisée |
| 3 | Conversion de types | `id` int, `price` float, `stock` int… |
| 4 | Normalisation des dates | ISO 8601 UTC (`created_at`, `updated_at`, `review_date`) |
| 5 | Filtrage | suppression des produits au prix ≤ 0 ou absent |
| 6 | Nouvelles colonnes | `price_after_discount`, `stock_level`, `price_range`, `sentiment` |
| 7 | Agrégation + jointure | `nb_reviews` et `avg_review_rating` calculés sur les avis puis joints aux produits |
| 8 | Aplatissement | un document par avis (index `reviews`), e-mail remplacé par un hash (anonymisation) |

## 7. Contrôles qualité (`scripts/validate.py`)
21 contrôles, rapport écrit dans `data/processed/quality_report.json` : nombre minimal de lignes, champs obligatoires, unicité de `id` et `review_id`, types, prix > 0, note dans [0,5] / [1,5], stock ≥ 0, prix remisé ≤ prix, intégrité référentielle avis → produits. Un seul échec fait échouer la tâche : **rien n'est chargé dans Elasticsearch**.

## 8. Mapping Elasticsearch (`elasticsearch/mapping.json`)
Deux index, `dynamic: strict` (tout champ inattendu est refusé), 1 shard, 0 réplica.

**`products`** : `id` integer · `title` text + keyword · `description` text · `category`, `brand`, `stock_level`, `price_range`, `availability_status`, `tags` keyword · `price`, `price_after_discount`, `discount_percentage`, `rating`, `avg_review_rating`, `weight` float · `stock`, `nb_reviews`, `min_order_quantity` integer · `created_at`, `updated_at`, `ingested_at` date.

**`reviews`** : `review_id` keyword (= `_id`) · `product_id` integer · `product_title` text + keyword · `category`, `brand`, `sentiment`, `reviewer_hash` keyword · `rating` integer · `comment` text · `review_date`, `ingested_at` date.

Le `_id` est déterministe : relancer le DAG **met à jour** les documents sans créer de doublons (chargement idempotent).

## 9. Analyses (`elasticsearch/queries.json`)
Exécutées par la tâche `analyze`, résultats dans `data/processed/analysis_results.json` et dans les logs.

| Requête | Question |
|---|---|
| `avg_price_by_category` | Prix moyen et volume par catégorie ? |
| `top_brands_by_review_rating` | Top 10 des marques les mieux notées ? |
| `stock_level_distribution` | Répartition des niveaux de stock ? |
| `out_of_stock_products` | Quels produits réapprovisionner en priorité ? |
| `avg_discount_by_category` | Quelles catégories sont le plus remisées ? |
| `sentiment_distribution` | Part d'avis positifs / neutres / négatifs ? |
| `reviews_per_month` | Évolution du volume et de la note des avis ? |
| `negative_reviews_by_category` | Où se concentrent les avis négatifs ? |

## 10. Dashboard Kibana
Dashboard **« Produits & Avis clients — Dashboard »** (`kibana/dashboard.ndjson`), importé automatiquement par le service `kibana-init` :

1. Nombre total de produits (metric)
2. Note moyenne des produits (metric)
3. Répartition des produits par catégorie (donut)
4. Sentiment des avis clients (donut)
5. Prix moyen par catégorie (barres)
6. Produits par niveau de stock (barres)
7. Évolution du nombre d'avis par mois (courbe)
8. Top 10 marques par note moyenne (tableau)

Le fichier est généré par `python kibana/generate_dashboard.py` ; il peut être ré-importé via *Stack Management → Saved Objects → Import*.

## 11. Lancer le projet

**Prérequis :** Docker + Docker Compose, ~4 Go de RAM libres.

```bash
git clone <URL_DU_DEPOT> && cd projet-data-engineering
cp .env.example .env              # Linux/macOS : echo "AIRFLOW_UID=$(id -u)" > .env
mkdir -p logs data/raw data/processed
docker compose up -d --build      # 1er démarrage : quelques minutes
docker compose ps                 # attendre que tout soit "healthy"/"running"
```

1. Ouvrir **Airflow** → http://localhost:8080 (`admin` / `admin`).
2. Activer puis lancer le DAG **`etl_products_reviews`** (bouton ▶ *Trigger DAG*).
3. Consulter les logs de chaque tâche (en particulier `validate` et `analyze`).
4. Ouvrir **Kibana** → http://localhost:5601 → *Analytics → Dashboard* → « Produits & Avis clients ».
   (Si besoin, ajuster la plage de temps : le dashboard est réglé sur 5 ans.)
5. Vérification rapide : `curl "localhost:9200/products/_count"` et `curl "localhost:9200/reviews/_count"`.

Arrêt : `docker compose down` (ajouter `-v` pour supprimer les données).

**Test sans Docker (extract/transform/validate seulement) :**
```bash
pip install -r requirements.txt
cd scripts && python -c "from extract import *; from transform import *; from validate import *; \
e=extract_products(); t=transform_products(e['raw_path']); print(validate_data(t['products_path'], t['reviews_path']))"
```

## 12. Captures d'écran
> À ajouter dans `docs/screenshots/` puis décommenter / adapter les liens :

| DAG Airflow (Graph) | Exécution réussie (Grid) |
|---|---|
| ![DAG](docs/screenshots/airflow_dag_graph.png) | ![Runs](docs/screenshots/airflow_dag_grid.png) |

| Dashboard Kibana |
|---|
| ![Dashboard](docs/screenshots/kibana_dashboard.png) |

## 13. Parcours d'une donnée
1. **Collectée** : à minuit, Airflow déclenche `extract` qui appelle l'API et sauvegarde le JSON brut dans `data/raw/`.
2. **Transformée** : `transform` déduplique, complète, convertit, normalise les dates, calcule de nouvelles colonnes et sépare produits / avis.
3. **Contrôlée** : `validate` exécute 21 contrôles ; en cas d'échec, le pipeline s'arrête.
4. **Stockée** : `load_documents` indexe en bulk dans Elasticsearch selon le mapping.
5. **Analysée** : `analyze` exécute les agrégations de `queries.json`.
6. **Visualisée** : Kibana lit les index et affiche le dashboard.

## 14. Dépannage
| Problème | Solution |
|---|---|
| Elasticsearch ne démarre pas (Linux) | `sudo sysctl -w vm.max_map_count=262144` |
| `Permission denied` sur `logs/` ou `data/` | vérifier `AIRFLOW_UID` dans `.env`, `chmod -R 777 logs data` |
| Dashboard vide | lancer le DAG au moins une fois ; vérifier la plage de temps Kibana |
| Dashboard absent | `docker compose run --rm kibana-init` |
| Tâche `extract` lente | API injoignable → fallback sur l'échantillon après 3 tentatives |
