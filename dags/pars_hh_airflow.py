from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2

# Функция для парсинга вакансий
def parse_vacancies():
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": "Python",
        "area": 1,  # 1 — Москва
        "professional_role": [96, 104],  # Программист, Разработчик
        "experience": "noExperience",  # Нет опыта
        "schedule": "remote",  # Удаленная работа
        "per_page": 100,  # Максимум 100 вакансий
        "date_from": (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),  # За последние 3 дня
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        vacancies = data.get("items", [])
        return vacancies
    else:
        raise Exception(f"Ошибка при запросе к API: {response.status_code}")

# Функция для сохранения данных в PostgreSQL
def save_to_db():
    vacancies = parse_vacancies()
    conn = psycopg2.connect(
        dbname='news_db',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    cursor = conn.cursor()

    for vacancy in vacancies:
        title = vacancy['name']
        company = vacancy['employer']['name']
        salary = str(vacancy.get('salary'))  # Зарплата может отсутствовать
        link = vacancy['alternate_url']
        published_at = datetime.strptime(vacancy['published_at'], "%Y-%m-%dT%H:%M:%S%z")

        cursor.execute(
            """
            INSERT INTO hh_pars (title, company, salary, link, published_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (title, company, salary, link, published_at)
        )

    conn.commit()
    cursor.close()
    conn.close()

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG
dag = DAG(
    'hh_vacancies_parser',
    default_args=default_args,
    description='Парсинг вакансий с hh.ru и сохранение в PostgreSQL',
    schedule_interval='0 0 */3 * *',  # Каждые 3 дня в 00:00
    catchup=False,
)

# Задача для парсинга и сохранения данных
parse_and_save = PythonOperator(
    task_id='parse_and_save',
    python_callable=save_to_db,
    dag=dag,
)

# Определение порядка задач
parse_and_save