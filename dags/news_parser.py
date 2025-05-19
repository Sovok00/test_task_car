from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import psycopg2

# Функция для парсинга новостей
def parse_news():
    url = 'https://ria.ru/'  # Замените на URL сайта с новостями
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    data = soup.find_all('div', class_='cell-list__item m-no-image')

    news_list = []
    for item in data:
        title = item.find('span').text.strip()
        link = item.find('a')['href']
        content = link  # Пока сохраняем только ссылку
        news_list.append((title, content))

    return news_list

# Функция для сохранения новостей в PostgreSQL
def save_to_db():
    news_list = parse_news()
    conn = psycopg2.connect(
        dbname='news_db',
        user='airflow',
        password='airflow',  # Замените на ваш пароль
        host='postgres',
        port='5432'
    )
    cursor = conn.cursor()
    for title, content in news_list:
        cursor.execute(
            "INSERT INTO news (title, content) VALUES (%s, %s)",
            (title, content)
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
    'news_parser',
    default_args=default_args,
    description='Ежедневный парсинг новостей и сохранение в PostgreSQL',
    schedule_interval='0 9 * * *',  # Ежедневно в 8:00 (UTC)
    catchup=False,
)

# Задача для парсинга и сохранения новостей
parse_and_save = PythonOperator(
    task_id='parse_and_save',
    python_callable=save_to_db,
    dag=dag,
)

# Определение порядка задач
parse_and_save