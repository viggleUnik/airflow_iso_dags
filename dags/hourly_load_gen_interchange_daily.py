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
    dag_id="hourly_gen_load_interchange",
    default_args={"retries": 4},
    tags=["iso_ne", "hourly_gen_load_interchange"],
    start_date=datetime(2023, 7, 10),
    catchup=False,
    schedule="@daily",
    max_active_runs=10
)
def import_hourly_load_gen_interchange():

    @task
    def get_hourly_load_gen_interchange_data():
        context = get_current_context()
        get_hourly_load_interchange_hook = HttpHook(
            http_conn_id='ISONE_REST_API',
            method='GET',
            auth_type=HTTPBasicAuth
        )

        dam_results = get_hourly_load_interchange_hook.run(
            endpoint=f"/hourlyloadgeninterchange/day/{context['ds_nodash']}"
        )
        results = dam_results.json()
        json_formatted_str = json.dumps(results, indent=2)

        logger.info('RESULTS: ')
        logger.info(json_formatted_str)
        return results

    @task
    def load_hourly_load_gen_interchange_data(hourly_load_interchange):
        context = get_current_context()

        import pandas as pd
        db_engine = PostgresHook(
            postgres_conn_id='local_postgres'
        ).get_sqlalchemy_engine()

        if hourly_load_interchange['HourlyLoadGenInterchanges']:
            d = {
                'begin_date': [a['BeginDate'] for a in
                               hourly_load_interchange['HourlyLoadGenInterchanges']['HourlyLoadGenInterchange']],
                'load': [a['Load'] for a in
                         hourly_load_interchange['HourlyLoadGenInterchanges']['HourlyLoadGenInterchange']],
                'native_load': [a['NativeLoad'] for a in
                                hourly_load_interchange['HourlyLoadGenInterchanges']['HourlyLoadGenInterchange']],
                'ard_demand': [a['ArdDemand'] for a in
                               hourly_load_interchange['HourlyLoadGenInterchanges']['HourlyLoadGenInterchange']],
                'total_generation': [a['TotalGeneration'] for a in
                                     hourly_load_interchange['HourlyLoadGenInterchanges']['HourlyLoadGenInterchange']],
                'total_interchange': [a['TotalInterchange'] for a in
                                      hourly_load_interchange['HourlyLoadGenInterchanges']['HourlyLoadGenInterchange']],

            }

            df = pd.DataFrame.from_dict(d, orient='columns')
            df.to_sql(
                con=db_engine,
                name='hourlyloadgeninterchange_raw',
                schema='public',
                if_exists='append',
                index=False)

    hourly_load_interchange = get_hourly_load_gen_interchange_data()
    load_hourly_load_gen_interchange_data(hourly_load_interchange)


import_hourly_load_gen_interchange()

