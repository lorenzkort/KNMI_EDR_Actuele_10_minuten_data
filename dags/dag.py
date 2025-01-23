from datetime import datetime, timedelta
import logging
import os
import traceback
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from src import (
    step_1_download,
    step_2_save_parquet,
    step_3_validate_parquet,
    step_4_timescale_ingest,
    time_delay
)

# Set up logging
logger = LoggingMixin().log
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

# Define logging callbacks
def task_success_logger(context):
    task = context['task_instance']
    logger.info(f"Task {task.task_id} completed successfully")
    logger.info(f"Task duration: {task.duration}")
    logger.info(f"Task end date: {task.end_date}")

def task_failure_logger(context):
    task = context['task_instance']
    exception = context.get('exception')
    logger.error(f"Task {task.task_id} failed")
    logger.error(f"Exception: {str(exception)}")
    logger.error(f"Traceback: {''.join(traceback.format_tb(exception.__traceback__))}")
    logger.error(f"Task duration: {task.duration}")
    logger.error(f"Task start date: {task.start_date}")
    logger.error(f"Task end date: {task.end_date}")
    
    # Log task context for debugging
    try:
        logger.error(f"Task parameters: {task.params}")
        logger.error(f"Previous task status: {context.get('prev_task_instance_state')}")
        logger.error(f"DAG run status: {context.get('dag_run').get_state()}")
    except Exception as e:
        logger.error(f"Error logging task context: {str(e)}")

def task_wrapper(python_callable):
    """Wrapper to ensure all exceptions are caught and logged"""
    def wrapper(*args, **kwargs):
        try:
            return python_callable(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {python_callable.__name__}: {str(e)}")
            logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
            raise
    return wrapper

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'knmi_weather_pipeline',
    default_args=default_args,
    description='Pipeline to process KNMI 10-minute weather data',
    schedule='*/10 * * * *',  # Run every 10 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather', 'knmi'],
) as dag:

    logger.info("Initialising KNMI weather pipeline DAG")

    download_task = PythonOperator(
        task_id='download_data',
        python_callable=task_wrapper(step_1_download.main),
        on_success_callback=task_success_logger,
        on_failure_callback=task_failure_logger,
    )

    save_parquet_task = PythonOperator(
        task_id='save_parquet',
        python_callable=task_wrapper(step_2_save_parquet.main),
        on_success_callback=task_success_logger,
        on_failure_callback=task_failure_logger,
    )

    validate_parquet_task = PythonOperator(
        task_id='validate_parquet',
        python_callable=task_wrapper(step_3_validate_parquet.main),
        on_success_callback=task_success_logger,
        on_failure_callback=task_failure_logger,
    )

    ingest_timescale_task = PythonOperator(
        task_id='ingest_timescale',
        python_callable=task_wrapper(step_4_timescale_ingest.main),
        on_success_callback=task_success_logger,
        on_failure_callback=task_failure_logger,
    )

    delay_task = PythonOperator(
        task_id='time_delay',
        python_callable=task_wrapper(time_delay.main),
        on_success_callback=task_success_logger,
        on_failure_callback=task_failure_logger,
    )

    logger.info("Setting up task dependencies")
    # Set task dependencies
    download_task >> save_parquet_task >> validate_parquet_task >> ingest_timescale_task

