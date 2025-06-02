CREATE DATABASE news_db;
GRANT ALL PRIVILEGES ON DATABASE news_db TO airflow;

-- Создание БД для Superset
CREATE DATABASE superset;
CREATE USER superset WITH PASSWORD 'superset';
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;

\c news_db

-- Основная таблица для хранения данных об автомобилях
CREATE TABLE IF NOT EXISTS car_prices (
    car_id SERIAL PRIMARY KEY,
    brand VARCHAR(100),
    model VARCHAR(100),
    engine_volume FLOAT,
    manufacture_year INTEGER,
    price_foreign FLOAT,
    price_rub FLOAT,
    file_actual_date DATE,  -- Дата актуальности файла CSV
    processing_date TIMESTAMP,  -- Дата и время обработки DAG
    UNIQUE (brand, model, engine_volume, manufacture_year, file_actual_date)
);

-- Архивная таблица для хранения исторических данных об автомобилях
CREATE TABLE IF NOT EXISTS car_prices_arch (
    car_id SERIAL PRIMARY KEY,
    brand VARCHAR(100),
    model VARCHAR(100),
    engine_volume FLOAT,
    manufacture_year INTEGER,
    price_foreign FLOAT,
    price_rub FLOAT,
    file_actual_date DATE,  -- Дата актуальности файла CSV
    processing_date TIMESTAMP,  -- Дата и время обработки DAG
    archive_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Временная таблица для хранения данных об автомобилях за текущий день
CREATE TABLE IF NOT EXISTS car_prices_day (
    car_id SERIAL PRIMARY KEY,
    brand VARCHAR(100),
    model VARCHAR(100),
    engine_volume FLOAT,
    manufacture_year INTEGER,
    price_foreign FLOAT,
    price_rub FLOAT,
    file_actual_date DATE,
    processing_date TIMESTAMP
);

-- Основная таблица для хранения курсов валют
CREATE TABLE IF NOT EXISTS currency_rates (
    rate_id SERIAL PRIMARY KEY,
    valute_id VARCHAR(10) NOT NULL,
    char_code VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    nominal INTEGER NOT NULL,
    value FLOAT NOT NULL,
    rate_date DATE NOT NULL,  -- Дата актуальности курса валют
    processing_date TIMESTAMP,  -- Дата и время обработки DAG
    UNIQUE (valute_id, rate_date)
);

-- Архивная таблица для хранения исторических курсов валют
CREATE TABLE IF NOT EXISTS currency_rates_arch (
    rate_id SERIAL PRIMARY KEY,
    valute_id VARCHAR(10) NOT NULL,
    char_code VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    nominal INTEGER NOT NULL,
    value FLOAT NOT NULL,
    rate_date DATE NOT NULL,
    processing_date TIMESTAMP,
    archive_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Временная таблица для хранения курсов валют за текущий день
CREATE TABLE IF NOT EXISTS currency_rates_day (
    rate_id SERIAL PRIMARY KEY,
    valute_id VARCHAR(10) NOT NULL,
    char_code VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    nominal INTEGER NOT NULL,
    value FLOAT NOT NULL,
    rate_date DATE NOT NULL,
    processing_date TIMESTAMP
);