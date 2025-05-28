CREATE DATABASE news_db;
GRANT ALL PRIVILEGES ON DATABASE news_db TO airflow;

-- Создание БД для Superset
CREATE DATABASE superset;
CREATE USER superset WITH PASSWORD 'superset';
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;

\c news_db

CREATE TABLE IF NOT EXISTS news (
    id SERIAL PRIMARY KEY,
    title TEXT,
    content TEXT
);

CREATE TABLE IF NOT EXISTS hh_pars (
    id SERIAL PRIMARY KEY,
    title TEXT,
    company TEXT,
    salary TEXT,
    link TEXT,
    published_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS car_prices (
    car_id SERIAL PRIMARY KEY,
    brand VARCHAR(100),
    model VARCHAR(100),
    engine_volume FLOAT,
    manufacture_year INTEGER,
    price_foreign FLOAT,
    price_rub FLOAT,
    file_actual_date DATE,
    processing_date TIMESTAMP,
    UNIQUE (brand, model, engine_volume, manufacture_year, file_actual_date)
);

-- Создание таблицы для курсов валют (если используется в вашем DAG)
CREATE TABLE IF NOT EXISTS cur_rate (
    valute_id VARCHAR(10) NOT NULL,
    char_code VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    nominal INTEGER NOT NULL,
    value FLOAT NOT NULL,
    file_update_date DATE NOT NULL,
    PRIMARY KEY (valute_id, file_update_date)
);