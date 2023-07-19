import json
import logging
import os
import requests
import tempfile
import pandas as pd


from airflow.exceptions import AirflowException
from pendulum import datetime
from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)

@dag(
    dag_id="rt_market_generator_lbmp_daily",
    default_args={"retries": 4},
    tags=["ny_iso", "rt_market_zonal_lbmp"],
    start_date=datetime(2023, 7, 17),
    catchup=False,
    schedule="@daily",
    max_active_runs=10
)
def import_rt_market_generator():

    @task
    def get_rt_market_generator_daily_data():
        context = get_current_context()

        url = f"http://mis.nyiso.com/public/csv/realtime/{context['ds_nodash']}realtime_gen.csv"
        response = requests.get(url)
        if response.status_code == 200:
            with tempfile.TemporaryFile(mode='w+b', suffix='.csv') as fp:
                fp.write(response.content)
                fp.seek(0)
                df = pd.read_csv(fp)
                return df
        else:
            raise AirflowException(f"No remote data. Status code: {response.status_code}")


    @task
    def load_rt_market_zonal_daily_data(realtime_data):
        db_engine = PostgresHook(
            postgres_conn_id='local_postgres'
        ).get_sqlalchemy_engine()

        column_names = {
            "Time Stamp": "time_stamp",
            "Name": "name",
            "PTID": "ptid",
            "LBMP ($/MWHr)": "lbmp",
            "Marginal Cost Losses ($/MWHr)": "marginal_cost_losses",
            "Marginal Cost Congestion ($/MWHr)": "marginal_cost_congestion"
        }

        df = realtime_data.rename(columns=column_names)
        df.to_sql(
            con=db_engine,
            name='rtmarket_generator_raw',
            schema='ny_iso',
            if_exists='append',
            index=False)

    realtime_data = get_rt_market_generator_daily_data()
    load_rt_market_zonal_daily_data(realtime_data)


import_rt_market_generator()
