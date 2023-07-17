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

# end_date for dag exec need to be 3 days ago from the current date to obtain data
@dag(
    dag_id='realtime_hourly_demand',
    default_args={"retries": 4},
    tags=["iso_ne", "realtime_h_demand"],
    start_date=datetime(2023, 7, 11),
    catchup=False,
    schedule="@daily",
    max_active_runs=10
)
def import_realtime_hourly_demand():
    @task
    def get_locations():
        locations_hook = PostgresHook(
            postgres_conn_id='local_postgres'
        )
        locations = locations_hook.get_records(sql='select zone_id from public.zones;')
        return locations

    @task
    def get_realtime_hourly_demand(locations):
        context = get_current_context()
        for location in locations:
            zone = location[0]
            if zone:
                logger.info(f"Checking location ID {location}")
                get_realtime_hourly_demand_hook = HttpHook(
                    http_conn_id='ISONE_REST_API',
                    method='GET',
                    auth_type=HTTPBasicAuth
                )
                dam_results = get_realtime_hourly_demand_hook.run(
                    endpoint=f"/realtimehourlydemand/day/{context['ds_nodash']}/location/{zone}"
                )
                results = dam_results.json()
                context['ti'].xcom_push(key=f"realtimehourlydemand_{zone}_{context['ds_nodash']}", value=results)
                logger.info(f"Received Results: {results}")

    @task
    def load_realtime_hourly_demand(locations, hourly_demands):
        context = get_current_context()
        import pandas as pd
        db_engine = PostgresHook(
            postgres_conn_id='local_postgres'
        ).get_sqlalchemy_engine()
        for location in locations:
            l = location[0]
            if l:
                dam_data = context['ti'].xcom_pull(key=f"realtimehourlydemand_{l}_{context['ds_nodash']}")
                logger.info(f'HourlyRTDem DATA: {dam_data}')
                data = {
                    'begin_date': [x['BeginDate'] for x in dam_data['HourlyRtDemands']['HourlyRtDemand']],
                    'location_id': [x['Location']['@LocId'] for x in dam_data['HourlyRtDemands']['HourlyRtDemand']],
                    'location_name': [x['Location']['$'] for x in dam_data['HourlyRtDemands']['HourlyRtDemand']],
                    'load': [x['Load'] for x in dam_data['HourlyRtDemands']['HourlyRtDemand']],
                }
                df = pd.DataFrame.from_dict(data, orient='columns')
                df.to_sql(
                    con=db_engine,
                    name='realtimehourlydemand_raw',
                    schema='public',
                    if_exists='append',
                    index=False)

    locations = get_locations()
    hourly_rt_demands = get_realtime_hourly_demand(locations)
    load_realtime_hourly_demand(locations, hourly_rt_demands)


import_realtime_hourly_demand()
