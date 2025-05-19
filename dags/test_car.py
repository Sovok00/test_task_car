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
    'test_task_car',
    default_args=default_args,
    description='Парсинг данных автомобилей и загрузка в PostgreSQL',
    schedule_interval='0 0 * * 1-7',  # Ежедневно в 00:00 с понедельника по воскресенье
    catchup=False,
    max_active_runs=1,
    tags=['car_prices'],
)


def process_file(**context):
    """
    Функция для обработки файла из MinIO.
    Читает CSV файл, преобразует в DataFrame и возвращает данные в виде словаря.
    """
    logger.info(">>> [START] process_file")
    try:
        execution_date = context['execution_date']
        logger.info(f"Execution date: {execution_date}")

        # Инициализация подключения к MinIO
        s3_hook = S3Hook(aws_conn_id='minio_conn')
        s3_client = s3_hook.get_conn()
        s3_client._endpoint.host = 'http://minio:9000'  # Явное указание хоста MinIO

        bucket = 'car-prices-bucket'
        file_key = f'cars_{execution_date.strftime("%Y%m%d")}.csv'

        # Проверка существования файла
        if not s3_hook.check_for_key(file_key, bucket):
            raise ValueError(f"File {file_key} not found in bucket {bucket}")

        # Чтение файла из MinIO
        file_obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        raw_data = file_obj['Body'].read()

        # Логирование первых байт для диагностики
        logger.info(f"File header: {raw_data[:20].hex()}")

        # Попытка чтения с разными кодировками
        for encoding in ['utf-8-sig', 'utf-16', 'windows-1251']:
            try:
                df = pd.read_csv(io.BytesIO(raw_data), encoding=encoding)

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

        # Возвращаем данные в виде словаря
        return df.to_dict('records')

    except Exception as e:
        logger.error(f"Error in process_file: {str(e)}", exc_info=True)
        raise


def load_to_postgres(**context):
    """
    Функция для загрузки данных в PostgreSQL.
    Получает данные из предыдущей задачи через XCom и загружает в БД.
    """
    logger.info(">>> [START] load_to_postgres")
    conn = None
    cursor = None

    try:
        # Получаем данные из предыдущей задачи
        records = context['ti'].xcom_pull(task_ids='process_file')
        if not records:
            raise ValueError("No data to load from XCom")

        # Подключение к PostgreSQL
        hook = PostgresHook(
            postgres_conn_id='postgres_default',
            schema='news_db'  # Явное указание базы данных
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

        # Подготовка и выполнение запроса на вставку для каждой записи
        insert_query = """
        INSERT INTO car_prices 
        (brand, model, engine_volume, manufacture_year, 
         price_foreign, price_rub, file_actual_date, processing_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """

        successful_inserts = 0

        for record in records:
            try:
                # Подготовка данных для вставки
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

        # Фиксация изменений
        conn.commit()
        logger.info(f"Successfully loaded {successful_inserts}/{len(records)} records to PostgreSQL")

    except Exception as e:
        logger.error(f"DB error: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        raise
    finally:
        # Всегда закрываем соединения
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
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

# Определение порядка выполнения задач
process_task >> load_task