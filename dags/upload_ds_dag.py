from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task_group
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from time import sleep


DAG_DEFAULT_ARGS = {'start_date': datetime(2025, 7, 1), 'depends_on_past': False}
DATA_PATH = '~/airflow/dags/data'
PG_CONN_USER_LOGS = 'PG_CONN_USER_LOGS'
PG_CONN_USER_DATA = 'PG_CONN_USER_DATA'
DAG_ID = "UPLOAD_DS"


def postgres_upsert(table, conn, keys, data_iter):
    LoggingMixin().log.info('\n <def postgres_upsert> : BEGIN \n')
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data)
    if table.table.name != 'ft_posting_f':
        stmt = stmt.on_conflict_do_update(
            constraint=f"{table.table.name}_pkey",
            set_={c.key: c for c in stmt.excluded},
    )
    result = conn.execute(stmt)
    LoggingMixin().log.info('\n <def postgres_upsert> : END \n')
    return result.rowcount

def file_to_table(filename, schema, table):
    LoggingMixin().log.info('\n <def file_to_table> : BEGIN \n')

    df = pd.read_csv(DATA_PATH + '/' + filename, sep=';', dtype=str, encoding = "utf-8")
    LoggingMixin().log.info(f'\n <def file_to_table> : {len(df)} records read from the file <{filename}> \n')
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates()
    LoggingMixin().log.info(f'\n <def file_to_table> : {len(df)} records are ready to upload \n')

    postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_USER_DATA)
    engine = postgres_hook.get_sqlalchemy_engine()
    resalt = df.to_sql(
        name=table,
        schema=schema,
        con=engine,
        index=False,
        if_exists='append',
        method=postgres_upsert
    )

    sleep(5)
    LoggingMixin().log.info('\n <def file_to_table> : END \n')
    return resalt


with DAG(
    dag_id=DAG_ID,
    description='Dag to transfer data from files to postgres',
    schedule_interval='@once',
    default_args=DAG_DEFAULT_ARGS,
    is_paused_upon_creation=True,
    max_active_runs=1,
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id='start'
    )

# ------ md_account_d ---------------------------------------------------------------------------
    with TaskGroup('group_upload_md_account_d') as group_upload_md_account_d:
        start_log_upload_md_account_d = PostgresOperator(
            task_id = 'start_log_upload_md_account_d',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'md_account_d', 'ds', 'STARTED', NULL);
            """
        )
        upload_md_account_d = PythonOperator(
            task_id = 'upload_md_account_d',
            python_callable=file_to_table,
            op_kwargs={
                'filename': 'md_account_d.csv',
                'schema': 'ds',
                'table': 'md_account_d'
            }
        )
        end_log_upload_md_account_d = PostgresOperator(
            task_id = 'end_log_upload_md_account_d',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'md_account_d', 'ds', 'COMPLETED',
                '{{ ti.xcom_pull(task_ids="group_upload_md_account_d.upload_md_account_d",
                                 key="return_value") }}' || ' records');
            """
        )
        start_log_upload_md_account_d >> \
        upload_md_account_d >> \
        end_log_upload_md_account_d

# ------ md_currency_d ---------------------------------------------------------------------------
    with TaskGroup('group_upload_md_currency_d') as group_upload_md_currency_d:
        start_log_upload_md_currency_d = PostgresOperator(
            task_id = 'start_log_upload_md_currency_d',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'md_currency_d', 'ds', 'STARTED', NULL);
            """
        )
        upload_md_currency_d = PythonOperator(
            task_id = 'upload_md_currency_d',
            python_callable=file_to_table,
            op_kwargs={
                'filename': 'md_currency_d.csv',
                'schema': 'ds',
                'table': 'md_currency_d'
            }
        )
        end_log_upload_md_currency_d = PostgresOperator(
            task_id = 'end_log_upload_md_currency_d',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'md_currency_d', 'ds', 'COMPLETED',
                '{{ ti.xcom_pull(task_ids="group_upload_md_currency_d.upload_md_currency_d",
                                 key="return_value") }}' || ' records');
            """
        )
        start_log_upload_md_currency_d >> \
        upload_md_currency_d >> \
        end_log_upload_md_currency_d

# ------ md_exchange_rate_d ---------------------------------------------------------------------------
    with TaskGroup('group_upload_md_exchange_rate_d') as group_upload_md_exchange_rate_d:
        start_log_upload_md_exchange_rate_d = PostgresOperator(
            task_id = 'start_log_upload_md_exchange_rate_d',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'md_exchange_rate_d', 'ds', 'STARTED', NULL);
            """
        )
        upload_md_exchange_rate_d = PythonOperator(
            task_id = 'upload_md_exchange_rate_d',
            python_callable=file_to_table,
            op_kwargs={
                'filename': 'md_exchange_rate_d.csv',
                'schema': 'ds',
                'table': 'md_exchange_rate_d'
            }
        )
        end_log_upload_md_exchange_rate_d = PostgresOperator(
            task_id = 'end_log_upload_md_exchange_rate_d',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'md_exchange_rate_d', 'ds', 'COMPLETED',
                '{{ ti.xcom_pull(task_ids="group_upload_md_exchange_rate_d.upload_md_exchange_rate_d",
                                 key="return_value") }}' || ' records');
            """
        )
        start_log_upload_md_exchange_rate_d >> \
        upload_md_exchange_rate_d >> \
        end_log_upload_md_exchange_rate_d

# ------ md_ledger_account_s ---------------------------------------------------------------------------
    with TaskGroup('group_upload_md_ledger_account_s') as group_upload_md_ledger_account_s:
        start_log_upload_md_ledger_account_s = PostgresOperator(
            task_id = 'start_log_upload_md_ledger_account_s',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'md_ledger_account_s', 'ds', 'STARTED', NULL);
            """
        )
        upload_md_ledger_account_s = PythonOperator(
            task_id = 'upload_md_ledger_account_s',
            python_callable=file_to_table,
            op_kwargs={
                'filename': 'md_ledger_account_s.csv',
                'schema': 'ds',
                'table': 'md_ledger_account_s'
            }
        )
        end_log_upload_md_ledger_account_s = PostgresOperator(
            task_id = 'end_log_upload_md_ledger_account_s',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'md_ledger_account_s', 'ds', 'COMPLETED',
                '{{ ti.xcom_pull(task_ids="group_upload_md_ledger_account_s.upload_md_ledger_account_s",
                                 key="return_value") }}' || ' records');
            """
        )
        start_log_upload_md_ledger_account_s >> \
        upload_md_ledger_account_s >> \
        end_log_upload_md_ledger_account_s

# ------ ft_balance_f ---------------------------------------------------------------------------
    with TaskGroup('group_upload_ft_balance_f') as group_upload_ft_balance_f:
        start_log_upload_ft_balance_f = PostgresOperator(
            task_id = 'start_log_upload_ft_balance_f',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'ft_balance_f', 'ds', 'STARTED', NULL);
            """
        )
        upload_ft_balance_f = PythonOperator(
            task_id = 'upload_ft_balance_f',
            python_callable=file_to_table,
            op_kwargs={
                'filename': 'ft_balance_f.csv',
                'schema': 'ds',
                'table': 'ft_balance_f'
            }
        )
        end_log_upload_ft_balance_f = PostgresOperator(
            task_id = 'end_log_upload_ft_balance_f',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'ft_balance_f', 'ds', 'COMPLETED',
                '{{ ti.xcom_pull(task_ids="group_upload_ft_balance_f.upload_ft_balance_f",
                                 key="return_value") }}' || ' records');
            """
        )
        start_log_upload_ft_balance_f >> \
        upload_ft_balance_f >> \
        end_log_upload_ft_balance_f

# ------ ft_posting_f ---------------------------------------------------------------------------
    with TaskGroup('group_upload_ft_posting_f') as group_upload_ft_posting_f:
        truncate_ft_posting_f = PostgresOperator(
            task_id = 'truncate_ft_posting_f',
            postgres_conn_id=PG_CONN_USER_DATA,
            sql='TRUNCATE TABLE ds.ft_posting_f;'
        )
        log_truncate_ft_posting_f = PostgresOperator(
            task_id = 'log_truncate_ft_posting_f',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'truncate table', 'ft_posting_f', 'ds', 'COMPLETED', NULL);
            """
        )
        start_log_upload_ft_posting_f = PostgresOperator(
            task_id = 'start_log_upload_ft_posting_f',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'ft_posting_f', 'ds', 'STARTED', NULL);
            """
        )
        upload_ft_posting_f = PythonOperator(
            task_id = 'upload_ft_posting_f',
            python_callable=file_to_table,
            op_kwargs={
                'filename': 'ft_posting_f.csv',
                'schema': 'ds',
                'table': 'ft_posting_f'
            }
        )
        end_log_upload_ft_posting_f = PostgresOperator(
            task_id = 'end_log_upload_ft_posting_f',
            postgres_conn_id=PG_CONN_USER_LOGS,
            sql="""
                INSERT INTO logs.data_logs(datetime, "event", table_name, schema_name, status, value)
                VALUES(CURRENT_TIMESTAMP, 'upload data', 'ft_posting_f', 'ds', 'COMPLETED',
                '{{ ti.xcom_pull(task_ids="group_upload_ft_posting_f.upload_ft_posting_f",
                                 key="return_value") }}' || ' records');
            """
        )
        truncate_ft_posting_f >> \
        log_truncate_ft_posting_f >> \
        start_log_upload_ft_posting_f >> \
        upload_ft_posting_f >> \
        end_log_upload_ft_posting_f


    end = EmptyOperator(
        task_id='end'
    )


start >> \
[group_upload_md_account_d, group_upload_md_currency_d, group_upload_md_exchange_rate_d,\
 group_upload_md_ledger_account_s, group_upload_ft_balance_f, group_upload_ft_posting_f]>> \
end