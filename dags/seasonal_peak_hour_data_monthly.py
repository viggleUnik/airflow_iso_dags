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
    dag_id='seasonal_peek_hour_data_monthly',
    default_args={"retries": 4},
    tags=["iso_ne"],
    start_date=datetime(2017, 1, 1),
    catchup=False,
    schedule="@monthly",
    max_active_runs=10
)


def import_seasonal_peak_hour_data():
    
    @task
    def get_seasonal_peak_hour_data():
        context = get_current_context()
        logger.info('CONTEXTUL:')
        logger.info(context['data_interval_start'].year)

        get_seasonal_peak_hour_data_hook = HttpHook(
            http_conn_id='ISONE_REST_API',
            method='GET',
            auth_type=HTTPBasicAuth
            )
        
        dam_results = get_seasonal_peak_hour_data_hook.run(
            endpoint=f"/seasonalpeakhourdata/month/{context['data_interval_start'].year}{context['data_interval_start'].month}"
            )
        results = dam_results.json()

        # json_formatted_str = json.dumps(results, indent=2)
        # print(json_formatted_str)

        logger.info('REZULTATE: ')
        logger.info(results)
        return results

    @task
    def load_seasonal_peak_hour_data(peak_data):
        context = get_current_context()
        import pandas as pd
        db_engine = PostgresHook(
            postgres_conn_id='local_postgres'
        ).get_sqlalchemy_engine()
        logger.info(f'PEAK DATA: {peak_data}')
        if peak_data['SeasonalPeakHoursData']:
            d = {'begin_date': [a['BeginDate'] for a in peak_data['SeasonalPeakHoursData']['SeasonalPeakHourData']],
                 'hourly_peak_load': [a['HourlyPeakLoad'] for a in peak_data['SeasonalPeakHoursData']['SeasonalPeakHourData']],
                 'system_peak_forecast': [a['SystemPeakForecast'] for a in peak_data['SeasonalPeakHoursData']['SeasonalPeakHourData']],
                 'settlement_status': [a['SettlementStatus'] for a in peak_data['SeasonalPeakHoursData']['SeasonalPeakHourData']],
                }
            df = pd.DataFrame.from_dict(d, orient='columns')
            df.to_sql(
                con=db_engine,
                name='seasonalpeakhourdata_raw',
                schema='public',
                if_exists='append',
                index=False)
            

    peak_data = get_seasonal_peak_hour_data()
    load_seasonal_peak_hour_data(peak_data)


import_seasonal_peak_hour_data()




