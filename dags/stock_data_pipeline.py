"""
Stock Market Data Pipeline DAG
Fetches stock data from Alpha Vantage API and stores it in PostgreSQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import logging
import os
import time
from typing import Dict, Optional

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

# DAG definition
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Fetch and store stock market data',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    tags=['stock-data', 'etl', 'finance'],
)

# Config
STOCK_SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
API_BASE_URL = 'https://www.alphavantage.co/query'


def get_api_key() -> str:
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is not set")
    return api_key


def create_table_if_not_exists(**context):
    """Create stock_data table if missing"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='stock_postgres')
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            open_price DECIMAL(10, 2),
            high_price DECIMAL(10, 2),
            low_price DECIMAL(10, 2),
            close_price DECIMAL(10, 2),
            volume BIGINT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        );
        CREATE INDEX IF NOT EXISTS idx_stock_symbol_timestamp 
        ON stock_data (symbol, timestamp);
        """
        postgres_hook.run(create_table_sql)
        logging.info("✅ stock_data table created or already exists")
    except Exception as e:
        logging.error(f"❌ Error creating table: {str(e)}")
        raise


def fetch_stock_data(symbol: str, retries: int = 3) -> Optional[Dict]:
    """Fetch stock data for a symbol with retry logic"""
    api_key = get_api_key()
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'interval': '60min',
        'apikey': api_key,
        'outputsize': 'compact'
    }

    for attempt in range(retries):
        try:
            logging.info(f"Fetching {symbol}, attempt {attempt+1}")
            response = requests.get(API_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")
            if 'Note' in data:
                logging.warning(f"API rate limit for {symbol}, retrying...")
                time.sleep(60)
                continue

            time_series_key = 'Time Series (60min)'
            if time_series_key not in data:
                raise ValueError(f"Expected key not found. Got: {list(data.keys())}")

            return {'symbol': symbol, 'data': data[time_series_key]}
        except Exception as e:
            logging.error(f"Error fetching {symbol}: {str(e)}")
            if attempt < retries - 1:
                time.sleep(10 * (attempt + 1))
            else:
                raise
    return None


def process_and_store_stock_data(**context):
    """Fetch, process, and insert stock data"""
    postgres_hook = PostgresHook(postgres_conn_id='stock_postgres')
    successful_updates = 0
    failed_updates = []

    for symbol in STOCK_SYMBOLS:
        try:
            if symbol != STOCK_SYMBOLS[0]:
                time.sleep(12)  # avoid API rate limit

            stock_data = fetch_stock_data(symbol)
            if not stock_data or not stock_data['data']:
                logging.warning(f"No data for {symbol}")
                failed_updates.append(symbol)
                continue

            records_to_insert = []
            for timestamp_str, values in stock_data['data'].items():
                try:
                    ts = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    records_to_insert.append({
                        'symbol': symbol,
                        'timestamp': ts,
                        'open_price': float(values.get('1. open', 0)),
                        'high_price': float(values.get('2. high', 0)),
                        'low_price': float(values.get('3. low', 0)),
                        'close_price': float(values.get('4. close', 0)),
                        'volume': int(values.get('5. volume', 0))
                    })
                except Exception as e:
                    logging.warning(f"Parse error {symbol} {timestamp_str}: {e}")

            insert_sql = """
            INSERT INTO stock_data (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
            VALUES (%(symbol)s, %(timestamp)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s, %(volume)s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                created_at = CURRENT_TIMESTAMP;
            """

            for record in records_to_insert:
                postgres_hook.run(insert_sql, parameters=record)

            logging.info(f"✅ {symbol}: {len(records_to_insert)} records inserted/updated")
            successful_updates += 1
        except Exception as e:
            logging.error(f"❌ Failed {symbol}: {e}")
            failed_updates.append(symbol)

    context['task_instance'].xcom_push(key='successful_updates', value=successful_updates)
    context['task_instance'].xcom_push(key='failed_updates', value=failed_updates)
    if successful_updates == 0:
        raise ValueError("No symbols updated!")


def validate_data_quality(**context):
    """Check inserted data quality"""
    postgres_hook = PostgresHook(postgres_conn_id='stock_postgres')
    count_sql = """
    SELECT COUNT(*) 
    FROM stock_data 
    WHERE created_at >= NOW() - INTERVAL '2 hours'
    """
    result = postgres_hook.get_first(count_sql)
    recent_count = result[0] if result else 0

    anomaly_sql = """
    SELECT symbol, COUNT(*), AVG(close_price), MIN(close_price), MAX(close_price)
    FROM stock_data 
    WHERE created_at >= NOW() - INTERVAL '2 hours'
    GROUP BY symbol
    """
    anomalies = postgres_hook.get_records(anomaly_sql)

    logging.info(f"✅ Data check: {recent_count} new records in last 2h")
    for symbol, count, avg, minp, maxp in anomalies:
        logging.info(f"{symbol}: {count} recs | avg={avg:.2f}, min={minp}, max={maxp}")
        if minp <= 0 or maxp <= 0:
            logging.warning(f"⚠ Suspicious prices for {symbol}")

    context['task_instance'].xcom_push(key='recent_count', value=recent_count)


# DAG tasks
create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table_if_not_exists,
    dag=dag,
)

fetch_and_store = PythonOperator(
    task_id='fetch_and_store',
    python_callable=process_and_store_stock_data,
    dag=dag,
)

validate_data = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data_quality,
    dag=dag,
)

# Task order
create_table >> fetch_and_store >> validate_data
