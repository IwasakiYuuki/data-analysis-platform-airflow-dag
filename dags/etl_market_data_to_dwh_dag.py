import pendulum
import datetime

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# # DAGデフォルト引数
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': pendulum.duration(minutes=5),
#     'conn_id': 'hiveserver2_default', # HiveServer2 接続IDに変更
# }

# DAG定義
with DAG(
    dag_id='elt_market_data_to_dwh_dag',
    # default_args=default_args,
    description='ELT DAG to load market data from Raw Zone to DWH (Fact/Dimension)',
    schedule='40 14 * * Mon',
    start_date=pendulum.datetime(2025, 4, 10),
    catchup=False, # 過去分の実行をスキップ
    tags=['DataWarehouse', 'MarketData'],
    template_searchpath='/home/airflow/airflow/airflow-dag/sqls'
) as dag:

    today = datetime.date.today()
    common_params = {
        'prev_week_monday': today - datetime.timedelta(days=today.weekday() + 7),
        'prev_week_friday': today - datetime.timedelta(days=today.weekday() + 3),
    }

    # --- Dimension 更新タスク ---

    upsert_dim_date = SQLExecuteQueryOperator(
        task_id='upsert_dim_date',
        sql='upsert_dim_date.sql',
        conn_id="hiveserver2_default",
        params=common_params
    )
    upsert_dim_instrument_stock = SQLExecuteQueryOperator(
        task_id='upsert_dim_instrument_stock',
        sql='upsert_dim_instrument_stock.sql',
        conn_id="hiveserver2_default",
        params=common_params
    )
    upsert_dim_instrument_forex = SQLExecuteQueryOperator(
        task_id='upsert_dim_instrument_forex',
        sql='upsert_dim_instrument_forex.sql',
        conn_id="hiveserver2_default",
        params=common_params
    )
    upsert_dim_instrument_index = SQLExecuteQueryOperator(
        task_id='upsert_dim_instrument_index',
        sql='upsert_dim_instrument_index.sql',
        conn_id="hiveserver2_default",
        params=common_params
    )

    # --- Fact 更新タスク ---
    # Dimension更新後に実行

    load_fact_market_data_stock = SQLExecuteQueryOperator(
        task_id='load_fact_market_data_stock',
        sql='insert_fact_market_data_stock.sql',
        conn_id="hiveserver2_default",
        params=common_params
    )
    load_fact_market_data_forex = SQLExecuteQueryOperator(
        task_id='load_fact_market_data_forex',
        sql='insert_fact_market_data_forex.sql',
        conn_id="hiveserver2_default",
        params=common_params
    )
    load_fact_market_data_index = SQLExecuteQueryOperator(
        task_id='load_fact_market_data_index',
        sql='insert_fact_market_data_index.sql',
        conn_id="hiveserver2_default",
        params=common_params
    )

    # --- タスク依存関係 ---
    # Dimension更新タスクは並行実行可能
    dimensions_updated = [upsert_dim_date, upsert_dim_instrument_stock, upsert_dim_instrument_forex, upsert_dim_instrument_index]

    # Fact更新タスクはDimension更新後に実行 (それぞれ並行実行可能)
    dimensions_updated >> load_fact_market_data_stock
    dimensions_updated >> load_fact_market_data_forex
    dimensions_updated >> load_fact_market_data_index
