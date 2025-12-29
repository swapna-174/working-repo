from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.dates import days_ago
import os
from gcp_bi_cl_etl_prime import bi_clinic_gcp_etl
import logging
import pendulum
from bi_cl_etl_file_processing import failure_callback, send_completion_email
 
local_tz= pendulum.timezone("America/New_York")

logger = logging.getLogger('airflow.task')
logger.setLevel(logging.DEBUG)

log = LoggingMixin().log
 
#project_id = 'rgclnp-p-l-cdm-processing1'
# project_id ='rgclnp-p-l-cdm-curated1'
 
config_dataset = 'CDW_STG'

task_application_nm = 'HB'
# task_priority_nbr= 2
task_priority_nbr= None


DP_KMS_KEY = os.environ.get("DP_KMS_KEY", "")
DP_REGION = os.environ.get("DP_REGION")
DP_ZONE = os.environ.get("DP_REGION") + "-b"
PROCESSING_SUBNET = os.environ.get("PROCESSING_SUBNET")
PROCESSING_VPC = os.environ.get("PROCESSING_VPC")
LAND_GCS = os.environ.get("LAND_GCS")
PROCESSING_GCS = os.environ.get("PROCESSING_GCS")
PROCESSING_SA = os.environ.get("PROCESSING_SA")
PROCESSING_PRJ = os.environ.get("PROCESSING_PRJ")
CLIN_SILVER_PRJ = os.environ.get("CLIN_SILVER_PRJ")
# CLIN_SILVER_GCS = os.environ.get("CLIN_SILVER_GCS")
CLIN_SILVER_GCS = "gs://extracts-silver-dev-rev" # AMS clinical file is part of rev cycle project, and this file will be saved in Rev Cycle bucket

CLIN_BRONZE_PRJ = os.environ.get("CLIN_BRONZE_PRJ")

project_id = CLIN_BRONZE_PRJ

bucket_project_id = CLIN_SILVER_PRJ #'extracts-silver-dev' #'rgclnp-p-l-cdm-processing1'
bucket_name = CLIN_SILVER_GCS[5:] # removes "gs://" prefix and creates "extracts-silver-dev-clin"


# DAG default arguments
default_dag_args={'owner': 'airflow',
                'start_date': pendulum.datetime(2025, 8, 27, 8, 20, tz=local_tz),
                'depends_on_past': False,
                'retry_delay': timedelta(minutes=2),
                'retries': 1,
                'email_on_failure': True,
                'email_on_retry': False,
                'on_failure_callback': failure_callback,
                 "dataflow_default_options": {      
                    'kmsKeyName': DP_KMS_KEY,
                    'workerZone': DP_ZONE,
                    'stagingLocation': PROCESSING_GCS + "/tmp",
                    'serviceAccountEmail': PROCESSING_SA,
                    'impersonate-service-account': PROCESSING_SA,
                    'projectId': PROCESSING_PRJ,
                },
            }

dag = DAG(
    'bi_cl_DSS_RevCycle_Monthly_dag',
    default_args=default_dag_args,
    description='A DAG to run BQ SP export data into GCS',
    schedule_interval="35 6 1 * *", # 35 — minute (at 35, i.e., on the hour)
                                    # 6 - Runs at 6 AM EST
                                    # * — every day of the month
                                    # * — every month
                                    # * — every day of the week
    catchup=False,
    tags=["BI_CLINICAL","EPIC","EPIC_EXTRACT","CLARITY"],
   ) 

 
task_grp_nm = 'RevCycle_Extracts_to_DSS'
 
export_task_DSS_BillAreaDim = PythonOperator(
    task_id='run_BigQuery_SP_export_to_gcs_DSS_BillAreaDim',
    python_callable=lambda: bi_clinic_gcp_etl(task_grp_nm,task_application_nm,1,bucket_name,config_dataset,project_id,bucket_project_id),
    dag=dag,
)



# Final task to send completion email
send_completion_notification = PythonOperator(
    task_id='send_completion_email',
    python_callable=send_completion_email,
    dag=dag,
)

( export_task_DSS_BillAreaDim >>  send_completion_notification 
 )