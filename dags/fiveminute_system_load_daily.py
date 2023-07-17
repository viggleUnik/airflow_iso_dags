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
    dag_id="fiveminute_system_load_daily",
    default_args={"retries": 4},
    tags=["iso_ne", "fiveminute_system_load"],
    start_date=datetime(2023, 7, 10),
    catchup=False,
    schedule="@daily",
    max_active_runs=10
)
def import_fiveminute_system_load():

    @task
    def get_fiveminute_system_load_data():
        context = get_current_context()
        get_fiveminute_system_load_data_hook = HttpHook(
            http_conn_id='ISONE_REST_API',
            method='GET',
            auth_type=HTTPBasicAuth
        )

        dam_results = get_fiveminute_system_load_data_hook.run(
            endpoint=f"/fiveminutesystemload/day/{context['ds_nodash']}"
        )
        results = dam_results.json()
        json_formatted_str = json.dumps(results, indent=2)

        logger.info('REZULTATE: ')
        logger.info(json_formatted_str)

        return results

    @task
    def load_fiveminute_system_load_data(fiveminute_load):
        context = get_current_context()

        import pandas as pd
        db_engine = PostgresHook(
            postgres_conn_id='local_postgres'
        ).get_sqlalchemy_engine()

        if fiveminute_load['FiveMinSystemLoads']:
            d = {
                'begin_date': [a['BeginDate'] for a in fiveminute_load['FiveMinSystemLoads']['FiveMinSystemLoad']],
                'load_mw': [a['LoadMw'] for a in fiveminute_load['FiveMinSystemLoads']['FiveMinSystemLoad']],
                'native_load': [a['NativeLoad'] for a in fiveminute_load['FiveMinSystemLoads']['FiveMinSystemLoad']],
                'ard_demand': [a['ArdDemand'] for a in fiveminute_load['FiveMinSystemLoads']['FiveMinSystemLoad']],
                'system_load_btm_pv': [a['SystemLoadBtmPv'] for a in
                                       fiveminute_load['FiveMinSystemLoads']['FiveMinSystemLoad']],
                'native_load_btm_pv': [a['NativeLoadBtmPv'] for a in
                                       fiveminute_load['FiveMinSystemLoads']['FiveMinSystemLoad']]
            }
            df = pd.DataFrame.from_dict(d, orient='columns')
            df.to_sql(
                con=db_engine,
                name='fiveminutesystemload_raw',
                schema='public',
                if_exists='append',
                index=False)

    five_minute_load = get_fiveminute_system_load_data()
    load_fiveminute_system_load_data(five_minute_load)


import_fiveminute_system_load()
