"""Витрина KarpovZone: обработка в Spark и отчёты в Greenplum."""

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Все настройки окружения собраны здесь, чтобы не искать их по коду.
K8S_NAMESPACE = "YOUR_NAMESPACE"
K8S_CONN_ID = "YOUR_KUBERNETES_CONNECTION_ID"
GREENPLUM_CONN_ID = "YOUR_GREENPLUM_CONNECTION_ID"
DB_SCHEMA = "YOUR_DB_SCHEMA"
S3_BUCKET = "YOUR_S3_BUCKET"
S3_PREFIX = "YOUR_S3_PREFIX"
PXF_SERVER = "default"

SELLER_ITEMS_LOCATION = (
    f"pxf://{S3_BUCKET}/{S3_PREFIX}/seller_items"
    f"?PROFILE=s3:parquet&SERVER={PXF_SERVER}"
)

SPARK_SUBMIT_TASK_ID = "submit_spark_job"

DEFAULT_ARGS = {
    "owner": "vladislav_ivanchenko",
}

with DAG(
    dag_id="karpovzone_data_mart",
    description="Обработка данных в Spark и создание витрины в Greenplum",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 9, 10, tz="UTC"),
    catchup=False,
    tags=["karpovzone", "spark", "greenplum", "etl"],
) as dag:
    submit_spark_job = SparkKubernetesOperator(
        task_id=SPARK_SUBMIT_TASK_ID,
        namespace=K8S_NAMESPACE,
        application_file="spark_submit.yaml",
        kubernetes_conn_id=K8S_CONN_ID,
        do_xcom_push=True,
    )

    # Беру имя запущенного приложения из XCom.
    wait_for_spark_job = SparkKubernetesSensor(
        task_id="wait_for_spark_job",
        namespace=K8S_NAMESPACE,
        application_name=(
            "{{ task_instance.xcom_pull(task_ids='"
            + SPARK_SUBMIT_TASK_ID
            + "')['metadata']['name'] }}"
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        attach_log=True,
    )

    # Greenplum читает Parquet из S3 через PXF, без копирования в БД.
    # CASCADE удалит зависимые представления: ниже создадаю их заново.
    create_seller_items_table = SQLExecuteQueryOperator(
        task_id="create_seller_items_table",
        conn_id=GREENPLUM_CONN_ID,
        sql=f"""
            DROP EXTERNAL TABLE IF EXISTS
                "{DB_SCHEMA}".seller_items CASCADE;

            CREATE EXTERNAL TABLE "{DB_SCHEMA}".seller_items (
                sku_id BIGINT,
                title TEXT,
                category TEXT,
                brand TEXT,
                seller TEXT,
                group_type TEXT,
                country TEXT,
                availability_items_count BIGINT,
                ordered_items_count BIGINT,
                warehouses_count BIGINT,
                item_price BIGINT,
                goods_sold_count BIGINT,
                item_rate FLOAT8,
                days_on_sell BIGINT,
                avg_percent_to_sold BIGINT,
                returned_items_count INTEGER,
                potential_revenue BIGINT,
                total_revenue BIGINT,
                avg_daily_sales FLOAT8,
                days_to_sold FLOAT8,
                item_rate_percent FLOAT8
            )
            LOCATION ('{SELLER_ITEMS_LOCATION}')
            ON ALL
            FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
            ENCODING 'UTF8';
        """,
    )


    create_unreliable_sellers_view = SQLExecuteQueryOperator(
        task_id="create_unreliable_sellers_view",
        conn_id=GREENPLUM_CONN_ID,
        sql=f"""
            DROP VIEW IF EXISTS "{DB_SCHEMA}".unreliable_sellers_view;

            CREATE VIEW "{DB_SCHEMA}".unreliable_sellers_view AS
            SELECT
                seller,
                SUM(availability_items_count) AS total_overload_items_count,
                CASE
                    WHEN SUM(days_on_sell) > 100
                        AND SUM(availability_items_count)
                            > SUM(ordered_items_count)
                    THEN TRUE
                    ELSE FALSE
                END AS is_unreliable
            FROM "{DB_SCHEMA}".seller_items
            GROUP BY seller;
        """,
    )

    create_brands_view = SQLExecuteQueryOperator(
        task_id="create_brands_view",
        conn_id=GREENPLUM_CONN_ID,
        sql=f"""
            DROP VIEW IF EXISTS "{DB_SCHEMA}".item_brands_view;

            CREATE VIEW "{DB_SCHEMA}".item_brands_view AS
            SELECT
                brand,
                group_type,
                country,
                SUM(potential_revenue) AS potential_revenue,
                SUM(total_revenue) AS total_revenue,
                COUNT(*) AS items_count
            FROM "{DB_SCHEMA}".seller_items
            GROUP BY brand, group_type, country;
        """,
    )

    # Отчёты независимы друг от друга и могут строиться параллельно.
    (
        submit_spark_job
        >> wait_for_spark_job
        >> create_seller_items_table
        >> [create_unreliable_sellers_view, create_brands_view]
    )
