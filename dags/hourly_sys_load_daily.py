import json
import logging
import os

from pendulum import datetime
from requests.auth import HTTPBasicAuth

from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

@dag(
    dag_id="hourly_sys_load_daily",
    default_args={"retries": 4},
    tags=["iso_ne", "hourly_sys_load"],
    start_date=datetime(2023, 7, 10),
    catchup=False,
    schedule="@daily",
    max_active_runs=10
)

def import_hourly_system_load():
    @task
    def get_hourly_sys_load_data():
        context = get_current_context()
        get_hourly_sys_load_data_hook = HttpHook(
            http_conn_id='ISONE_REST_API',
            method='GET',
            auth_type=HTTPBasicAuth
        )

        dam_results = get_hourly_sys_load_data_hook.run(
            endpoint=f"/hourlysysload/day/{context['ds_nodash']}"
        )
        results = dam_results.json()
        json_formatted_str = json.dumps(results, indent=2)

        logger.info('REZULTATE: ')
        logger.info(json_formatted_str)
        return results

    @task
    def load_hourly_sys_load_data(hourly_sys_load):
        context = get_current_context()

        import pandas as pd
        db_engine = PostgresHook(
            postgres_conn_id='local_postgres'
        ).get_sqlalchemy_engine()

        if hourly_sys_load['HourlySystemLoads']:
            d = {
                'begin_date': [a['BeginDate'] for a in hourly_sys_load['HourlySystemLoads']['HourlySystemLoad']],
                'location': [a['Location']['$'] for a in hourly_sys_load['HourlySystemLoads']['HourlySystemLoad']],
                'load': [a['Load'] for a in hourly_sys_load['HourlySystemLoads']['HourlySystemLoad']],
                'native_load': [a['NativeLoad'] for a in hourly_sys_load['HourlySystemLoads']['HourlySystemLoad']],
                'ard_demand': [a['ArdDemand'] for a in hourly_sys_load['HourlySystemLoads']['HourlySystemLoad']]
            }

            df = pd.DataFrame.from_dict(d, orient='columns')
            df.to_sql(
                con=db_engine,
                name='hourlysystemload_raw',
                schema='public',
                if_exists='append',
                index=False)

    hourly_system_load = get_hourly_sys_load_data()
    load_hourly_sys_load_data(hourly_system_load)


import_hourly_system_load()







