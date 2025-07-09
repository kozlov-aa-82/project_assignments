from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


DAG_DEFAULT_ARGS = {
    'start_date': datetime(2018, 1, 1),
    'end_date': datetime(2018, 1, 31),
    'depends_on_past': False
}
PG_CONN_USER_DATA = 'PG_CONN_USER_DATA'
DAG_ID = "UPDATE_dm_account_turnover_f"


with DAG(
    dag_id=DAG_ID,
    description='Updating the data mart dm_account_turnover_f',
    schedule_interval='@daily',
    default_args=DAG_DEFAULT_ARGS,
    is_paused_upon_creation=True,
    max_active_runs=1,
    catchup=True
) as dag:

    update_dm_account_turnover_f = PostgresOperator(
        task_id = 'update_dm_account_turnover_f',
        postgres_conn_id=PG_CONN_USER_DATA,
        sql="CALL ds.fill_account_turnover_f('{{ ds }}');"
    )
        
update_dm_account_turnover_f