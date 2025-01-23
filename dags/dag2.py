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

step_1_download.main()
step_2_save_parquet.main()
step_3_validate_parquet.main()
step_4_timescale_ingest.main()
time_delay.main()