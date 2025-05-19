from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from airflow.utils.log.logging_mixin import LoggingMixin
import io
import psycopg2
import requests
from dateutil.relativedelta import relativedelta

logger = LoggingMixin().log

# Отключаем проверку метаданных EC2 для AWS
os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Определение DAG
dag = DAG(
    'car_prices_etl',
    default_args=default_args,
    description='ETL для данных об автомобилях с конвертацией в рубли',
    schedule_interval='0 0 * * 1-6',  # С понедельника по субботу в 00:00
    catchup=False,
    max_active_runs=1,
    tags=['car_prices', 'etl'],
)


def get_currency_rate(execution_date):
    """
    Получает курс валюты от ЦБ РФ на указанную дату.
    Для субботы использует курс на предыдущую пятницу.
    """
    if execution_date.weekday() == 5:  # Суббота
        execution_date = execution_date - timedelta(days=1)
        if execution_date.weekday() == 5:  # Если предыдущий день тоже суббота
            execution_date = execution_date - timedelta(days=1)

    date_str = execution_date.strftime('%d/%m/%Y')
    url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}'

    try:
        response = requests.get(url)
        response.raise_for_status()

        # Парсим XML (здесь упрощенно, в реальности нужно использовать xml.etree.ElementTree)
        # Предположим, что валюта - USD и ее код - R01235
        usd_rate = float(
            response.text.split('<Valute ID="R01010">')[1].split('<Value>')[1].split('</Value>')[0].replace(',', '.'))
        return usd_rate
    except Exception as e:
        logger.error(f"Failed to get currency rate: {str(e)}")
        raise


def process_file(**context):
    """
    Функция для обработки файла из S3.
    Читает CSV файл, конвертирует цены в рубли и возвращает данные.
    """
    logger.info(">>> [START] process_file")
    try:
        execution_date = context['execution_date']
        logger.info(f"Execution date: {execution_date}")

        # Получаем курс валюты
        currency_rate = get_currency_rate(execution_date)
        logger.info(f"Currency rate: {currency_rate}")

        # Инициализация подключения к S3
        s3_hook = S3Hook(aws_conn_id='minio_conn')
        s3_client = s3_hook.get_conn()
        s3_client._endpoint.host = 'http://minio:9000'

        bucket = 'car-prices-bucket'
        file_key = f'cars_{execution_date.strftime("%Y%m%d")}.csv'

        # Проверка существования файла
        if not s3_hook.check_for_key(file_key, bucket):
            raise ValueError(f"File {file_key} not found in bucket {bucket}")

        # Чтение файла из S3
        file_obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        raw_data = file_obj['Body'].read()

        # Попытка чтения с разными кодировками
        for encoding in ['utf-8-sig', 'utf-16', 'windows-1251']:
            try:
                df = pd.read_csv(io.BytesIO(raw_data), encoding=encoding)

                # Конвертация цены в рубли
                df['price_rub'] = df['price_foreign'] * currency_rate

                # Добавляем дату обработки
                df['processing_date'] = execution_date.isoformat()

                # Преобразуем дату из файла в правильный формат
                df['file_actual_date'] = pd.to_datetime(df['file_actual_date']).dt.date

                logger.info(f"Successfully read with {encoding}")
                logger.info(f"Data sample:\n{df.head()}")
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError("Failed to decode file with tested encodings")

        return df.to_dict('records')

    except Exception as e:
        logger.error(f"Error in process_file: {str(e)}", exc_info=True)
        raise


def load_to_greenplum(**context):
    """
    Функция для загрузки данных в GreenPlum.
    """
    logger.info(">>> [START] load_to_greenplum")
    conn = None
    cursor = None

    try:
        # Получаем данные из предыдущей задачи
        records = context['ti'].xcom_pull(task_ids='process_file')
        if not records:
            raise ValueError("No data to load from XCom")

        # Подключение к GreenPlum (используем PostgresHook, так как GreenPlum основан на PostgreSQL)
        hook = PostgresHook(
            postgres_conn_id='postgres_default',
            schema='news_db'
        )
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Создание таблицы (если не существует)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS car_prices (
            car_id SERIAL PRIMARY KEY,
            brand VARCHAR(100) NOT NULL,
            model VARCHAR(100) NOT NULL,
            engine_volume FLOAT NOT NULL,
            manufacture_year INTEGER NOT NULL,
            price_foreign FLOAT,
            price_rub FLOAT,
            file_actual_date DATE NOT NULL,
            processing_date TIMESTAMP NOT NULL,
            UNIQUE (brand, model, engine_volume, manufacture_year, file_actual_date)
        )
        """)
        conn.commit()

        # Подготовка запроса на вставку
        insert_query = """
        INSERT INTO car_prices 
        (brand, model, engine_volume, manufacture_year, 
         price_foreign, price_rub, file_actual_date, processing_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (brand, model, engine_volume, manufacture_year, file_actual_date) 
        DO UPDATE SET 
            price_foreign = EXCLUDED.price_foreign,
            price_rub = EXCLUDED.price_rub,
            processing_date = EXCLUDED.processing_date
        """

        successful_inserts = 0

        for record in records:
            try:
                data = (
                    record['brand'],
                    record['model'],
                    float(record['engine_volume']),
                    int(record['manufacture_year']),
                    float(record['price_foreign']),
                    float(record['price_rub']),
                    record['file_actual_date'],
                    record['processing_date']
                )
                cursor.execute(insert_query, data)
                successful_inserts += 1
            except Exception as e:
                logger.error(f"Error inserting record: {record}. Error: {str(e)}")

        conn.commit()
        logger.info(f"Successfully loaded {successful_inserts}/{len(records)} records to GreenPlum")

    except Exception as e:
        logger.error(f"DB error: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Определение задач
process_task = PythonOperator(
    task_id='process_file',
    python_callable=process_file,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_greenplum',
    python_callable=load_to_greenplum,
    provide_context=True,
    dag=dag,
)

# Определение порядка выполнения задач
process_task >> load_task