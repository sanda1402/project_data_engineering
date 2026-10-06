"""DAG ETL : produits & avis clients -> Elasticsearch -> Kibana.

check_elasticsearch >> extract >> transform >> validate >> [create_indices >> load] >> analyze
"""
import logging
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Les scripts sont montés dans /opt/airflow/scripts (voir docker-compose.yml)
sys.path.insert(0, "/opt/airflow/scripts")

from analyze import run_analyses          # noqa: E402
from config import DEFAULT_API_URL        # noqa: E402
from extract import extract_products      # noqa: E402
from load import check_connection, create_indices, load_documents  # noqa: E402
from transform import transform_products  # noqa: E402
from validate import DataQualityError, validate_data  # noqa: E402

log = logging.getLogger(__name__)
ES_CONN_ID = "elasticsearch_default"


def _es_url() -> str:
    """Construit l'URL Elasticsearch à partir de la Connection Airflow."""
    conn = BaseHook.get_connection(ES_CONN_ID)
    scheme = conn.conn_type if conn.conn_type in ("http", "https") else "http"
    return f"{scheme}://{conn.host}:{conn.port or 9200}"


def _notify_failure(context):
    ti = context["task_instance"]
    log.error("ÉCHEC de la tâche %s (run %s) : %s", ti.task_id, context["run_id"], context.get("exception"))


# --- Callables ---------------------------------------------------------------
def _check_es(**_):
    return check_connection(_es_url())


def _extract(ds, **_):
    return extract_products(
        api_url=Variable.get("products_api_url", default_var=DEFAULT_API_URL),
        use_fallback=Variable.get("use_fallback", default_var="true").lower() == "true",
        run_date=ds,
    )


def _transform(ti, **_):
    raw = ti.xcom_pull(task_ids="extract")          # XCom : chemin du fichier brut
    return transform_products(raw["raw_path"])


def _validate(ti, **_):
    t = ti.xcom_pull(task_ids="transform")
    try:
        return validate_data(t["products_path"], t["reviews_path"],
                             min_rows=int(Variable.get("min_rows", default_var="10")))
    except DataQualityError as exc:
        # Inutile de réessayer : les données sont mauvaises, pas l'infrastructure
        raise AirflowFailException(str(exc)) from exc


def _create_indices(**_):
    return create_indices(_es_url(), recreate=False)


def _load(ti, **_):
    t = ti.xcom_pull(task_ids="transform")
    return load_documents(_es_url(), t["products_path"], t["reviews_path"])


def _analyze(**_):
    return run_analyses(_es_url())


# --- DAG ---------------------------------------------------------------------
default_args = {
    "owner": "data-engineer",
    "retries": 2,                               # retries en cas d'échec
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": _notify_failure,
}

with DAG(
    dag_id="etl_products_reviews",
    description="ETL produits & avis clients : API -> Elasticsearch -> Kibana",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",                          # planification quotidienne
    catchup=False,
    max_active_runs=1,
    tags=["etl", "elasticsearch", "kibana", "projet-final"],
    doc_md=__doc__,
) as dag:

    check_elasticsearch = PythonOperator(task_id="check_elasticsearch", python_callable=_check_es)
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    validate = PythonOperator(task_id="validate", python_callable=_validate)

    with TaskGroup(group_id="elasticsearch_load") as es_group:
        create_idx = PythonOperator(task_id="create_indices", python_callable=_create_indices)
        load = PythonOperator(task_id="load_documents", python_callable=_load)
        create_idx >> load

    analyze = PythonOperator(task_id="analyze", python_callable=_analyze)

    check_elasticsearch >> extract >> transform >> validate >> es_group >> analyze
