from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta
from twitter_collect_script import TwitterRequests


pipeline = TwitterRequests()

DEFAULT_ARGS = {
    'retries': 2,
    'owner':'airflow',
    'email_on_retry': False,
    'depends_on_past': False,
    'email_on_failure': True,
    'start_date':  datetime(2022, 6, 13),
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'TwitterDataCollect',
    description="Simple ETL to Work with Twitter API",
    catchup=False,
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(days=1)
)

# Jobs Definition

create_buckets_task = PythonOperator(
    task_id='create_s3_buckets',
    python_callable=pipeline.create_buckets,
    op_args=['twitterlake'],
    dag=dag
)

twitter_api_request_task = BranchPythonOperator(
    task_id='twitter_api_request',
    python_callable=pipeline.api_request,
    provide_context=True,
    dag=dag
)

api_success_task = DummyOperator(
    task_id='api_success_request_all_data',
    dag=dag
)

api_error_task = DummyOperator(
    task_id='api_error_request_all_data',
    dag=dag
)

load_dataset_task = PythonOperator(
    task_id='load_json_files',
    python_callable=pipeline.load_data,
    trigger_rule='none_failed_or_skipped',
    provide_context=True,
    dag=dag
)

get_data_tasks = [PythonOperator(
    task_id=f'get_{task}_data',
    python_callable=function,
    provide_context=True,
    dag=dag
) for task, function in zip(
    ['users', 'posts', 'retweets'], 
    [pipeline.get_users, pipeline.get_posts, pipeline.get_retweets]
)]

store_datasets_task = PythonOperator(
    task_id='store_datasets_on_s3',
    python_callable=pipeline.store_dataset,
    dag=dag
)

create_buckets_task >> twitter_api_request_task >> [api_success_task, api_error_task] >> load_dataset_task >> get_data_tasks >> store_datasets_task