from airflow import DAG
from airflow.models.param import Param  
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
import os

# ALERT EMAIL LIBRARY
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ETL FOLDER
from weather_ETL.extract import extract_data
from weather_ETL.transform import transform_data
from weather_ETL.load import load_data

# DATASET PATH CONFIG
DATA_DIR = '/opt/airflow/data' 
RAW_FILE = os.path.join(DATA_DIR, 'raw_weather.json')
TRANSFORMED_FILE = os.path.join(DATA_DIR, 'transformed_weather.json')

# EMAIL ALERT CONFIG (FROM .env)
load_dotenv()
EMAIL_SENDER = os.getenv(key = 'EMAIL_SENDER')
APP_PASSWORD = os.getenv(key = 'EMAIL_PASSWORD')
EMAIL_RECEIVER = os.getenv(key = 'EMAIL_RECEIVER')

# SEND EMAIL SETUP
def send_email(subject, html_content):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER

    # ISI DARI EMAIL
    msg.attach(MIMEText(html_content, _subtype = "html"))

    # SEND EMAIL VIA SMTP
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL_SENDER, APP_PASSWORD)
        server.send_message(msg)

# --- EMAIL TEMPLATE IF PIPELINE SUCCESS ---
def build_success_email(context):
    return f"""<html>
                    <body>
                        <h2 style="color:green;">ETL SUCCESS ✅</h2>
                        <p>DAG: {context['dag'].dag_id}</p>
                        <p>Run ID: {context['run_id']}</p>
                    </body>
                </html>
            """

# --- EMAIL TEMPLATE IF PIPELINE FAIL
def build_failure_email(context):
    error = str(context.get("exception"))
    return f"""<html>   
                    <body>
                        <h2 style="color:red;">ETL FAILED ❌</h2>
                        <p>DAG: {context['dag'].dag_id}</p>
                        <p>Task: {context['task_instance'].task_id}</p>
                        <p>Error: {error}</p>
                    </body>
                </html>
            """

# IF ETL SUCCESS 
def success_callback(context):
    html = build_success_email(context)
    send_email("ETL Success", html)

# IF ETL FAIL
def failure_callback(context):
    html = build_failure_email(context)
    send_email("ETL Failed", html)



# DAG CONFIG PARAMS
default_args = {'owner': 'data_engineer',
                'depends_on_past': False,
                'start_date': datetime(2026, 3, 30),
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5)}

# BUILD DAG
with DAG(dag_id = 'Weather_ETL_API',
         description = 'Create ETL from Weather API',
         start_date = datetime(year = 2026, month = 3, day = 25),
         end_date = datetime(year = 2026, month = 6, day = 1),
         schedule = '@hourly',
         catchup = False, 
         fail_fast = True, 
         is_paused_upon_creation = False,
         tags = ['ETL', 'Weather', 'API'],
         default_args = default_args,
         on_failure_callback = failure_callback,  # GLOBAL FAILURE (IF ANY OF THOSE TASKS FAILS)
         params = {"cities_input": Param(default=["Jakarta", "Bogor", "Depok", "Tangerang", "Bekasi", "Cirebon", "Serang", 
                                                  "Surabaya", "Bandung", "Medan", "Makassar", "Denpasar", "Malang", "Tasikmalaya",
                                                  "Semarang", "Yogyakarta", "Solo", "Batam", "Lampung", "Maluku", "Manado"],
                                         type="array",
                                         description="Masukkan daftar kota di Indonesia yang ingin ditarik datanya. Contoh: [\"Denpasar\", \"Malang\"]"
                                         )
                   }) as dag:

    # ETL TASK
    extract_task = PythonOperator(task_id = 'extract', 
                                  python_callable = extract_data, 
                                  op_kwargs = {'output_path' : RAW_FILE})
    transform_task = PythonOperator(task_id = 'transform', 
                                    python_callable = transform_data, 
                                    op_kwargs = {'input_path' : RAW_FILE, 'output_path' : TRANSFORMED_FILE})
    load_task = PythonOperator(task_id = 'load', 
                               python_callable = load_data, 
                               on_success_callback = success_callback,  # SUCCESS ALERT
                               op_kwargs = {'input_path' : TRANSFORMED_FILE})

    # ETL WORKFLOW
    extract_task >> transform_task >> load_task


