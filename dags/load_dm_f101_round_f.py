from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


DAG_DEFAULT_ARGS = {
    'start_date': datetime(2018, 2, 1),
    'end_date': datetime(2018, 2, 1),
    'depends_on_past': False
}
PG_CONN_USER_DATA = 'PG_CONN_USER_DATA'
DAG_ID = "UPDATE_dm_f101_round_f"


with DAG(
    dag_id=DAG_ID,
    description='Updating the data mart dm_f101_round_f',
    schedule_interval='@monthly',
    default_args=DAG_DEFAULT_ARGS,
    is_paused_upon_creation=True,
    max_active_runs=1,
    catchup=True
) as dag:

    update_dm_f101_round_f = PostgresOperator(
        task_id = 'update_dm_account_balance_f',
        postgres_conn_id=PG_CONN_USER_DATA,
        sql="CALL dm.fill_f101_round_f('{{ ds }}');"
    )

update_dm_f101_round_f