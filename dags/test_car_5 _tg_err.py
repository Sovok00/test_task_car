from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from airflow.utils.log.logging_mixin import LoggingMixin
import io
import requests
import time
import xml.etree.ElementTree as ET
from airflow.hooks.base import BaseHook

logger = LoggingMixin().log

# Отключаем проверку метаданных EC2 для AWS
os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'

# Telegram Bot Settings
telegram_conn = BaseHook.get_connection('telegram_default')
TELEGRAM_BOT_TOKEN = telegram_conn.extra_dejson['token']
TELEGRAM_CHAT_ID = telegram_conn.extra_dejson['chat_id']

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
    'test_car_5_tg_err',
    default_args=default_args,
    description='ETL для данных об автомобилях с конвертацией в рубли',
    schedule_interval='0 0 * * 1-6',  # С понедельника по субботу в 00:00
    catchup=False,
    max_active_runs=1,
    tags=['car_prices', 'etl'],
)


def send_telegram_message(message):
    """Отправляет сообщение в Telegram чат"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Telegram notification sent successfully")
    except Exception as e:
        logger.error(f"Failed to send Telegram notification: {str(e)}")


def get_currency_rate(execution_date):
    """
    Получает все курсы валют от ЦБ РФ на указанную дату.
    Для субботы использует курс на предыдущую пятницу.
    В случае ошибки делает до 3 попыток с интервалом 15 сек.
    Возвращает курс BYN (R01090B).
    """
    if execution_date.weekday() == 5:  # Суббота
        execution_date = execution_date - timedelta(days=1)
        if execution_date.weekday() == 5:
            execution_date = execution_date - timedelta(days=1)

    date_str = execution_date.strftime('%d/%m/%Y')
    url = f'https://www.cbrr.ru/scripts/XML_daily.asp?date_req={date_str}'
    max_retries = 3
    retry_delay = 15  # в секундах
    last_error = None
    usd_rate = None  # Будем хранить курс отдельно

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.text)

            # Подключение к БД
            hook = PostgresHook(postgres_conn_id='postgres_default')
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Создаем таблицу, если она не существует
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cur_rate (
                    valute_id VARCHAR(10) NOT NULL,
                    char_code VARCHAR(10) NOT NULL,
                    name VARCHAR(100) NOT NULL,
                    nominal INTEGER NOT NULL,
                    value FLOAT NOT NULL,
                    file_update_date DATE NOT NULL,
                    PRIMARY KEY (valute_id, file_update_date)
                )
            """)
            conn.commit()

            # Обрабатываем все валюты из ответа
            for valute in root.findall('Valute'):
                valute_id = valute.get('ID')
                char_code = valute.find('CharCode').text
                name = valute.find('Name').text
                nominal = int(valute.find('Nominal').text)
                value = float(valute.find('Value').text.replace(',', '.'))

                # Сохраняем курс в базу
                cursor.execute("""
                    INSERT INTO cur_rate 
                    (valute_id, char_code, name, nominal, value, file_update_date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (valute_id, file_update_date) 
                    DO UPDATE SET 
                        char_code = EXCLUDED.char_code,
                        name = EXCLUDED.name,
                        nominal = EXCLUDED.nominal,
                        value = EXCLUDED.value
                """, (valute_id, char_code, name, nominal, value, execution_date.date()))

                # Запоминаем курс BYN для возврата из функции
                if valute_id == 'R01090B':
                    usd_rate = value / nominal

            conn.commit()

            if usd_rate is None:
                raise ValueError("Currency with ID R01090B (BYN) not found in response")

            return usd_rate

        except Exception as e:
            last_error = e
            logger.warning(f"Attempt {attempt} failed to get currency rates: {str(e)}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            continue
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()

    # Если все попытки провалились
    error_message = (
        f"⚠️ <b>Ошибка в DAG test_car_5_tg_err</b>\n"
        f"Не удалось получить курсы валют после {max_retries} попыток\n"
        f"Последняя ошибка: {str(last_error) if last_error else 'Неизвестная ошибка'}\n"
        f"Дата выполнения: {execution_date}"
    )
    send_telegram_message(error_message)
    logger.error(error_message)
    raise Exception(error_message)


def process_file(**context):
    """
    Функция для обработки файла из S3.
    Читает CSV файл, конвертирует цены в рубли и возвращает данные.
    """
    logger.info(">>> [START] process_file")
    try:
        execution_date = context['execution_date']
        logger.info(f"Execution date: {execution_date}")

        # Получаем курс валюты (без fallback)
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
            error_msg = f"Файл {file_key} не найден в бакете {bucket}"
            send_telegram_message(f"❌ <b>Ошибка в DAG test_car_5_tg_err</b>\n{error_msg}")
            raise ValueError(error_msg)

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
            error_msg = "Не удалось декодировать файл с тестируемыми кодировками"
            send_telegram_message(f"❌ <b>Ошибка в DAG test_car_5_tg_err</b>\n{error_msg}")
            raise ValueError(error_msg)

        return df.to_dict('records')

    except Exception as e:
        error_msg = f"Ошибка в process_file: {str(e)}"
        send_telegram_message(f"❌ <b>Ошибка в DAG test_car_5_tg_err</b>\n{error_msg}")
        logger.error(f"Error in process_file: {str(e)}", exc_info=True)
        raise


def load_to_postgres(**context):
    """
    Функция для загрузки данных в postgres.
    """
    logger.info(">>> [START] load_to_postgres")
    conn = None
    cursor = None

    try:
        # Получаем данные из предыдущей задачи
        records = context['ti'].xcom_pull(task_ids='process_file')
        if not records:
            error_msg = "Нет данных для загрузки из XCom"
            send_telegram_message(f"❌ <b>Ошибка в DAG test_car_5_tg_err</b>\n{error_msg}")
            raise ValueError(error_msg)

        # Подключение к postgres
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
        success_msg = (
            f"✅ <b>DAG test_car_5_tg_err успешно выполнен</b>\n"
            f"Загружено записей: {successful_inserts}/{len(records)}\n"
            f"Дата выполнения: {context['execution_date']}"
        )
        send_telegram_message(success_msg)
        logger.info(f"Successfully loaded {successful_inserts}/{len(records)} records to postgres")

    except Exception as e:
        error_msg = f"Ошибка в load_to_postgres: {str(e)}"
        send_telegram_message(f"❌ <b>Ошибка в DAG test_car_5_tg_err</b>\n{error_msg}")
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
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

# Определение порядка выполнения задач
process_task >> load_task