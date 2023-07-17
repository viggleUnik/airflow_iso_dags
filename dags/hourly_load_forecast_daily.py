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
    dag_id="hourly_load_forecast_daily",
    default_args={"retries": 4},
    tags=["iso_ne", "hourly_forecast"],
    start_date=datetime(2023, 7, 10),
    catchup=False,
    schedule="@daily",
    max_active_runs=10
)


def import_hourly_load_forecast():

    @task
    def get_hourly_load_forecast_data():
        context = get_current_context()
        get_hourly_load_forecast_data_hook = HttpHook(
            http_conn_id='ISONE_REST_API',
            method='GET',
            auth_type=HTTPBasicAuth
        )

        dam_results = get_hourly_load_forecast_data_hook.run(
            endpoint=f"/hourlyloadforecast/day/{context['ds_nodash']}"
        )
        results = dam_results.json()
        json_formatted_str = json.dumps(results, indent=2)

        logger.info('REZULTATE: ')
        logger.info(json_formatted_str)
        return results


    @task
    def load_hourly_load_forecast_data(hourly_forecast):
        context = get_current_context()

        import pandas as pd
        db_engine = PostgresHook(
            postgres_conn_id='local_postgres'
        ).get_sqlalchemy_engine()

        if hourly_forecast['HourlyLoadForecasts']:
            d = {
                'begin_date': [a['BeginDate'] for a in hourly_forecast['HourlyLoadForecasts']['HourlyLoadForecast']],
                'creation_date': [a['CreationDate'] for a in hourly_forecast['HourlyLoadForecasts']['HourlyLoadForecast']],
                'load_mw': [a['LoadMw'] for a in hourly_forecast['HourlyLoadForecasts']['HourlyLoadForecast']],
                'net_load_mw': [a['NetLoadMw'] for a in hourly_forecast['HourlyLoadForecasts']['HourlyLoadForecast']]
            }

            df = pd.DataFrame.from_dict(d, orient='columns')
            df.to_sql(
                con=db_engine,
                name='hourlyloadforecast_raw',
                schema='public',
                if_exists='append',
                index=False)

    hourly_forecast = get_hourly_load_forecast_data()
    load_hourly_load_forecast_data(hourly_forecast)


import_hourly_load_forecast()


