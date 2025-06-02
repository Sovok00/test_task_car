from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
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
from airflow.models import Variable

logger = LoggingMixin().log

# Отключаем проверку метаданных EC2 для AWS
os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'

# Telegram Bot Settings
telegram_conn = BaseHook.get_connection('telegram_default')
TELEGRAM_BOT_TOKEN = telegram_conn.extra_dejson['token']
TELEGRAM_CHAT_ID = telegram_conn.extra_dejson['chat_id']

# Конфигурация
DEFAULT_CURRENCY = 'R01090B'  # BYN
MAX_RETRIES = 3
RETRY_DELAY = timedelta(minutes=1)
DATA_RETENTION_DAYS = 30

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 14),
    'retries': 2,
    'retry_delay': RETRY_DELAY,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Определение DAG
dag = DAG(
    'test_car_5_tg',
    default_args=default_args,
    description='ETL процесс для данных об автомобилях с конвертацией в рубли',
    schedule_interval='10 8,14,18 * * 1-6',  # С понедельника по субботу в 00:00
    catchup=False,
    max_active_runs=1,
    tags=['car_prices', 'etl'],
    doc_md=__doc__,
)


def send_telegram_message(message, disable_notification=False):
    """Отправляет сообщение в Telegram чат"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML',
        'disable_notification': str(disable_notification).lower()
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Telegram notification sent successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to send Telegram notification: {str(e)}")
        return False


def notify_start(**context):
    """Уведомление о начале выполнения DAG"""
    execution_date = context['execution_date']
    message = (
        f"🚀 <b>Запущен DAG test_car_5_tg</b>\n"
        f"Дата выполнения: {execution_date}\n"
        f"Попытка: {context['ti'].try_number}"
    )
    return send_telegram_message(message)


def notify_success(**context):
    """Уведомление об успешном выполнении DAG"""
    execution_date = context['execution_date']
    dag_run = context['dag_run']

    # Вычисляем время выполнения только если есть start_date и end_date
    duration = ""
    if dag_run.start_date and dag_run.end_date:
        duration = f"\nВремя выполнения: {dag_run.end_date - dag_run.start_date}"
    elif dag_run.start_date:
        duration = f"\nDAG начал выполнение: {dag_run.start_date}"

    message = (
        f"✅ <b>DAG test_car_5_tg успешно завершен</b>\n"
        f"Дата выполнения: {execution_date}"
        f"{duration}"
    )
    return send_telegram_message(message)


def notify_failure(context):
    """Уведомление о неудачном выполнении DAG"""
    execution_date = context['execution_date']
    task_instance = context['task_instance']
    exception = context.get('exception', 'Неизвестная ошибка')

    message = (
        f"❌ <b>Ошибка в DAG test_car_5_tg</b>\n"
        f"Задача: {task_instance.task_id}\n"
        f"Ошибка: {str(exception)}\n"
        f"Дата выполнения: {execution_date}\n"
        f"Попытка: {task_instance.try_number}/{task_instance.max_tries + 1}"
    )
    return send_telegram_message(message, disable_notification=False)


def examination_s3(**context):
    """
    Проверяет наличие файла в S3, копирует его в архивный бакет с новым именем,
    загружает данные во временную таблицу car_prices_day
    """
    execution_date = context['execution_date']
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    s3_client = s3_hook.get_conn()
    s3_client._endpoint.host = 'http://minio:9000'

    source_bucket = 'car-prices-bucket'
    archive_bucket = 'car-prices-archive'
    file_key = f'cars_{execution_date.strftime("%Y%m%d")}.csv'
    new_file_key = f'cars_{execution_date.strftime("%Y%m%d")}_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'

    try:
        # Проверяем наличие файла в исходном бакете
        if not s3_hook.check_for_key(file_key, source_bucket):
            error_msg = f"Файл {file_key} отсутствует в бакете {source_bucket}"
            send_telegram_message(f"⚠️ <b>Внимание</b>\n{error_msg}")
            raise ValueError(error_msg)

        # Копируем файл в архивный бакет с новым именем
        s3_client.copy_object(
            Bucket=archive_bucket,
            Key=new_file_key,
            CopySource={'Bucket': source_bucket, 'Key': file_key}
        )
        logger.info(f"File copied to archive bucket: {new_file_key}")

        # Загружаем файл из архивного бакета
        file_obj = s3_client.get_object(Bucket=archive_bucket, Key=new_file_key)
        raw_data = file_obj['Body'].read()

        # Парсим CSV файл
        for encoding in ['utf-8-sig', 'utf-16', 'windows-1251']:
            try:
                df = pd.read_csv(io.BytesIO(raw_data), encoding=encoding)

                # Валидация данных
                required_columns = ['brand', 'model', 'engine_volume',
                                    'manufacture_year', 'price_foreign', 'file_actual_date']
                if not all(col in df.columns for col in required_columns):
                    raise ValueError(f"Отсутствуют обязательные колонки: {required_columns}")

                # Преобразование типов
                df['engine_volume'] = df['engine_volume'].astype(float)
                df['manufacture_year'] = df['manufacture_year'].astype(int)
                df['price_foreign'] = df['price_foreign'].astype(float)
                df['file_actual_date'] = pd.to_datetime(df['file_actual_date']).dt.date

                # Исправление: преобразуем execution_date в строку перед добавлением в DataFrame
                # df['processing_date'] = execution_date.strftime('%Y-%m-%d %H:%M:%S')
                df['processing_date'] = datetime.now()

                # Загружаем данные во временную таблицу
                hook = PostgresHook(postgres_conn_id='postgres_default')
                engine = hook.get_sqlalchemy_engine()

                # Очищаем временную таблицу перед загрузкой
                with engine.begin() as conn:
                    conn.execute("TRUNCATE TABLE car_prices_day")

                # Загружаем данные
                df.to_sql(
                    'car_prices_day',
                    engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )

                logger.info(f"Successfully loaded {len(df)} records to car_prices_day")
                return True

            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error processing data with {encoding} encoding: {str(e)}")
                raise

        raise ValueError("Не удалось декодировать файл с тестируемыми кодировками")

    except Exception as e:
        error_msg = f"Ошибка в задаче examination_s3: {str(e)}"
        send_telegram_message(f"❌ <b>Ошибка</b>\n{error_msg}")
        raise


def examination_cbr(**context):
    """
    Проверяет наличие данных по курсу валют на сайте ЦБ,
    загружает их во временную таблицу currency_rates_day
    """
    execution_date = context['execution_date']

    # Для субботы используем курс на предыдущую пятницу
    if execution_date.weekday() == 5:  # Суббота
        execution_date = execution_date - timedelta(days=1)
        if execution_date.weekday() == 5:
            execution_date = execution_date - timedelta(days=1)

    date_str = execution_date.strftime('%d/%m/%Y')
    url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}'

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        root = ET.fromstring(response.text)

        # Ищем нужную валюту
        for valute in root.findall('Valute'):
            if valute.get('ID') == DEFAULT_CURRENCY:
                nominal = int(valute.find('Nominal').text)
                value = float(valute.find('Value').text.replace(',', '.'))
                rate = value / nominal

                # Подготавливаем данные для вставки
                data = {
                    'valute_id': valute.get('ID'),
                    'char_code': valute.find('CharCode').text,
                    'name': valute.find('Name').text,
                    'nominal': nominal,
                    'value': value,
                    'rate_date': execution_date.date(),  # Преобразуем в date
                    'processing_date': datetime.now()  # Используем текущее время вместо execution_date
                }

                # Загружаем данные во временную таблицу
                hook = PostgresHook(postgres_conn_id='postgres_default')
                engine = hook.get_sqlalchemy_engine()

                # Очищаем временную таблицу перед загрузкой
                with engine.begin() as conn:
                    conn.execute("TRUNCATE TABLE currency_rates_day")

                # Загружаем данные
                pd.DataFrame([data]).to_sql(
                    'currency_rates_day',
                    engine,
                    if_exists='append',
                    index=False
                )

                logger.info(f"Successfully loaded currency rate for {valute.find('CharCode').text}")
                return True

        error_msg = f"Currency with ID {DEFAULT_CURRENCY} not found in response"
        send_telegram_message(f"⚠️ <b>Внимание</b>\n{error_msg}")
        raise ValueError(error_msg)

    except Exception as e:
        error_msg = f"Ошибка в задаче examination_cbr: {str(e)}"
        send_telegram_message(f"❌ <b>Ошибка</b>\n{error_msg}")
        raise


def association_car_cbr(**context):
    """
    Сравнивает данные из временных таблиц с основными,
    обновляет основные таблицы при необходимости,
    архивирует данные из временных таблиц
    """
    execution_date = context['execution_date']
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()

    current_time = datetime.now()  # Получаем текущее время для processing_date

    try:
        with engine.begin() as conn:
            # Проверяем car_prices_day
            car_count = conn.execute("SELECT COUNT(*) FROM car_prices_day").scalar()
            if car_count == 0:
                error_msg = "Нет данных в таблице car_prices_day"
                send_telegram_message(f"⚠️ <b>Внимание</b>\n{error_msg}")
                raise ValueError(error_msg)

            # Проверяем currency_rates_day
            rate_count = conn.execute("SELECT COUNT(*) FROM currency_rates_day").scalar()
            if rate_count == 0:
                error_msg = "Нет данных в таблице currency_rates_day"
                send_telegram_message(f"⚠️ <b>Внимание</b>\n{error_msg}")
                raise ValueError(error_msg)

            # Получаем курс валюты
            rate_result = conn.execute("""
                SELECT value / nominal as rate 
                FROM currency_rates_day 
                LIMIT 1
            """)
            rate = rate_result.scalar()

            if not rate:
                error_msg = "Не удалось получить курс валюты"
                send_telegram_message(f"❌ <b>Ошибка</b>\n{error_msg}")
                raise ValueError(error_msg)

            # Обновляем цены в рублях во временной таблице
            conn.execute("""
                UPDATE car_prices_day 
                SET price_rub = price_foreign * %s
            """, (rate,))

            # Проверяем, есть ли новые данные
            new_data_count = conn.execute("""
                SELECT COUNT(*) 
                FROM car_prices_day cpd
                LEFT JOIN car_prices cp ON 
                    cpd.brand = cp.brand AND 
                    cpd.model = cp.model AND 
                    cpd.engine_volume = cp.engine_volume AND 
                    cpd.manufacture_year = cp.manufacture_year AND 
                    cpd.file_actual_date = cp.file_actual_date
                WHERE cp.car_id IS NULL OR 
                    (cp.price_foreign != cpd.price_foreign OR cp.price_rub != cpd.price_rub)
            """).scalar()

            if new_data_count == 0:
                send_telegram_message("ℹ️ <b>Информация</b>\nСвежих данных не появилось")
                return True

            # Обновляем основную таблицу car_prices
            conn.execute("""
                            INSERT INTO car_prices 
                            (brand, model, engine_volume, manufacture_year, 
                             price_foreign, price_rub, file_actual_date, processing_date)
                            SELECT 
                                brand, model, engine_volume, manufacture_year, 
                                price_foreign, price_rub, file_actual_date, %s
                            FROM car_prices_day
                            ON CONFLICT (brand, model, engine_volume, manufacture_year, file_actual_date) 
                            DO UPDATE SET 
                                price_foreign = EXCLUDED.price_foreign,
                                price_rub = EXCLUDED.price_rub,
                                processing_date = EXCLUDED.processing_date
                        """, (current_time,))

            # Обновляем основную таблицу currency_rates
            conn.execute("""
                            INSERT INTO currency_rates 
                            (valute_id, char_code, name, nominal, value, rate_date, processing_date)
                            SELECT 
                                valute_id, char_code, name, nominal, value, rate_date, %s
                            FROM currency_rates_day
                            ON CONFLICT (valute_id, rate_date) 
                            DO UPDATE SET 
                                char_code = EXCLUDED.char_code,
                                name = EXCLUDED.name,
                                nominal = EXCLUDED.nominal,
                                value = EXCLUDED.value,
                                processing_date = EXCLUDED.processing_date
                        """, (current_time,))

            # Архивируем данные из временных таблиц с текущим временем
            conn.execute("""
                            INSERT INTO car_prices_arch 
                            (brand, model, engine_volume, manufacture_year, 
                             price_foreign, price_rub, file_actual_date, processing_date)
                            SELECT 
                                brand, model, engine_volume, manufacture_year, 
                                price_foreign, price_rub, file_actual_date, %s
                            FROM car_prices_day
                        """, (current_time,))

            conn.execute("""
                            INSERT INTO currency_rates_arch 
                            (valute_id, char_code, name, nominal, value, rate_date, processing_date)
                            SELECT 
                                valute_id, char_code, name, nominal, value, rate_date, %s
                            FROM currency_rates_day
                        """, (current_time,))

            # Гарантированная очистка временных таблиц
            logger.info("Truncating temporary tables...")
            conn.execute("TRUNCATE TABLE car_prices_day")
            conn.execute("TRUNCATE TABLE currency_rates_day")
            logger.info("Temporary tables truncated successfully")
            # row_count = conn.execute("SELECT COUNT(*) FROM car_prices_day").scalar()
            # logger.info(f"car_prices_day now contains {row_count} rows (should be 0)")

            # Отправляем уведомление об успешном обновлении
            send_telegram_message(
                f"✅ <b>Данные успешно обновлены</b>\n"
                f"Обновлено записей: {new_data_count}\n"
                f"Дата выполнения: {execution_date}"
            )

            return True

    except Exception as e:
        error_msg = f"Ошибка в задаче association_car_cbr: {str(e)}"
        send_telegram_message(f"❌ <b>Ошибка</b>\n{error_msg}")
        raise


# Start and end tasks
start_task = PythonOperator(
    task_id='start_notification',
    python_callable=notify_start,
    provide_context=True,
    dag=dag,
)

end_task = PythonOperator(
    task_id='end_notification',
    python_callable=notify_success,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

# Основные задачи
examination_s3_task = PythonOperator(
    task_id='examination_s3',
    python_callable=examination_s3,
    provide_context=True,
    dag=dag,
)

examination_cbr_task = PythonOperator(
    task_id='examination_cbr',
    python_callable=examination_cbr,
    provide_context=True,
    dag=dag,
)

association_car_cbr_task = PythonOperator(
    task_id='association_car_cbr',
    python_callable=association_car_cbr,
    provide_context=True,
    dag=dag,
)


# Настройка зависимостей
start_task >> [examination_s3_task, examination_cbr_task] >> association_car_cbr_task >> end_task