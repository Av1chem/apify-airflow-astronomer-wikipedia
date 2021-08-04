import os
import json
import logging

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


NUM_WORKERS = 4


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('apify_airflow_dag',
         start_date=datetime(2019, 1, 1),
         max_active_runs=3,
         schedule_interval=None,  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    oJoin = DummyOperator(
        task_id='join'
    )

    @task(multiple_outputs=True)
    def split(configs):
        configs_count = len(configs['queries'])
        step = configs_count / NUM_WORKERS
        start_idx = 0
        end_idx = 0
        partitions = {}
        for i in range(NUM_WORKERS):
            end_idx = min(start_idx + step, configs_count)
            partitions[i] = [int(start_idx // 1), int(end_idx // 1)]
            start_idx = end_idx
        if partitions:
            partitions[NUM_WORKERS - 1][-1] = configs_count
        work = {
            str(i): configs['queries'][partitions[i][0]:partitions[i][1]] for i in range(len(partitions))
        }
        logging.info(f"Broke configs into following partitions: {work}")
        return work

    splitted_work = split(json.loads(os.getenv('WIKI_CONFIG')))

    @task
    def pull(configs):
        for cfg in configs:
            resp = requests.post(
                url=f'https://api.apify.com/v2/acts/J4XmiHyPz2qoCyQr8/run-sync-get-dataset-items?token={os.getenv("APIFY_TOKEN")}&timeout=60',
                headers={
                  'Content-Type': 'application/json'
                },
                data=json.dumps({
                    "searchTerms": configs
                })
            ).json()
            logging.info(resp)
        return resp

    for part in range(NUM_WORKERS):
        splitted_work >> pull(
            configs=splitted_work[str(part)]
        ) >> oJoin
