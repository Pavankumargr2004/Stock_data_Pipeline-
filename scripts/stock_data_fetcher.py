#!/usr/bin/env python3
"""
Stock Market Data Fetcher Script
Standalone script for fetching stock data and updating PostgreSQL database
Can be run independently or called by Airflow
"""

import os
import sys
import logging
import time
import requests
import psycopg2
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from contextlib import contextmanager
import argparse
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/tmp/stock_fetcher.log') if os.path.exists('/tmp') else logging.NullHandler()
    ]
)

logger = logging.getLogger(__name__)


@dataclass
class StockRecord:
    """Data class for stock record"""
    symbol: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int


class StockDataFetcher:
    """Main class for fetching and storing stock market data"""
    
    def __init__(self):
        self.api_key = self._get_api_key()
        self.db_config = self._get_db_config()
        self.api_base_url = 'https://www.alphavantage.co/query'
        self.default_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        
    def _get_api_key(self) -> str:
        """Retrieve API key from environment variables"""
        api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        if not api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is not set")
        return api_key
    
    def _get_db_config(self) -> Dict[str, str]:
        """Get database connection configuration"""
        return {
            'host': os.getenv('STOCK_DB_HOST', 'localhost'),
            'database': os.getenv('STOCK_DB_NAME', 'stockdb'),
            'user': os.getenv('STOCK_DB_USER', 'stockuser'),
            'password': os.getenv('STOCK_DB_PASSWORD', 'stockpassword'),
            'port': int(os.getenv('STOCK_DB_PORT', 5432))
        }
    
    @contextmanager
    def get_db_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = False
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def create_table_if_not_exists(self) -> bool:
        """Create the stock_data table if it doesn't exist"""
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
        
        CREATE INDEX IF NOT EXISTS idx_stock_created_at 
        ON stock_data (created_at);
        """
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
                    logger.info("Stock data table created or verified successfully")
                    return True
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            return False
    
    def fetch_stock_data(self, symbol: str, retries: int = 3) -> Optional[Dict]:
        """Fetch stock data for a given symbol with retry logic"""
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol,
            'interval': '60min',
            'apikey': self.api_key,
            'outputsize': 'compact'
        }
        
        for attempt in range(retries):
            try:
                logger.info(f"Fetching data for {symbol} (attempt {attempt + 1}/{retries})")
                
                response = requests.get(self.api_base_url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Handle API errors
                if 'Error Message' in data:
                    raise ValueError(f"API Error: {data['Error Message']}")
                
                if 'Note' in data:
                    logger.warning(f"API Note for {symbol}: {data['Note']}")
                    if attempt < retries - 1:
                        time.sleep(60)  # Wait 1 minute before retry
                        continue
                    else:
                        raise ValueError(f"API rate limit exceeded for {symbol}")
                
                # Validate data structure
                time_series_key = 'Time Series (60min)'
                if time_series_key not in data:
                    available_keys = list(data.keys())
                    raise ValueError(f"Expected key '{time_series_key}' not found. Available keys: {available_keys}")
                
                return {
                    'symbol': symbol,
                    'data': data[time_series_key],
                    'metadata': data.get('Meta Data', {})
                }
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for {symbol} (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    time.sleep(10 * (attempt + 1))  # Exponential backoff
                else:
                    raise
            
            except Exception as e:
                logger.error(f"Unexpected error for {symbol} (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    time.sleep(10)
                else:
                    raise
        
        return None
    
    def parse_stock_data(self, stock_data: Dict) -> List[StockRecord]:
        """Parse raw stock data into StockRecord objects"""
        records = []
        symbol = stock_data['symbol']
        time_series = stock_data['data']
        
        for timestamp_str, values in time_series.items():
            try:
                # Parse timestamp
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                
                # Extract values with validation
                open_price = self._safe_float(values.get('1. open'))
                high_price = self._safe_float(values.get('2. high'))
                low_price = self._safe_float(values.get('3. low'))
                close_price = self._safe_float(values.get('4. close'))
                volume = self._safe_int(values.get('5. volume'))
                
                # Validate data quality
                if not all([open_price > 0, high_price > 0, low_price > 0, close_price > 0, volume >= 0]):
                    logger.warning(f"Invalid data for {symbol} at {timestamp_str}")
                    continue
                
                records.append(StockRecord(
                    symbol=symbol,
                    timestamp=timestamp,
                    open_price=open_price,
                    high_price=high_price,
                    low_price=low_price,
                    close_price=close_price,
                    volume=volume
                ))
                
            except (ValueError, KeyError) as e:
                logger.warning(f"Error parsing data point for {symbol} at {timestamp_str}: {e}")
                continue
        
        return records
    
    def _safe_float(self, value: str) -> float:
        """Safely convert string to float"""
        try:
            return float(value) if value else 0.0
        except (ValueError, TypeError):
            return 0.0
    
    def _safe_int(self, value: str) -> int:
        """Safely convert string to int"""
        try:
            return int(float(value)) if value else 0
        except (ValueError, TypeError):
            return 0
    
    def store_stock_records(self, records: List[StockRecord]) -> Tuple[int, int]:
        """Store stock records in the database using UPSERT"""
        if not records:
            return 0, 0
        
        insert_sql = """
        INSERT INTO stock_data (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) 
        DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            created_at = CURRENT_TIMESTAMP;
        """
        
        inserted_count = 0
        updated_count = 0
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for record in records:
                        try:
                            # Check if record exists
                            check_sql = """
                            SELECT id FROM stock_data 
                            WHERE symbol = %s AND timestamp = %s
                            """
                            cursor.execute(check_sql, (record.symbol, record.timestamp))
                            exists = cursor.fetchone() is not None
                            
                            # Insert/Update record
                            cursor.execute(insert_sql, (
                                record.symbol,
                                record.timestamp,
                                record.open_price,
                                record.high_price,
                                record.low_price,
                                record.close_price,
                                record.volume
                            ))
                            
                            if exists:
                                updated_count += 1
                            else:
                                inserted_count += 1
                                
                        except psycopg2.Error as e:
                            logger.error(f"Error storing record for {record.symbol}: {e}")
                            continue
                    
                    conn.commit()
                    logger.info(f"Database operation completed: {inserted_count} inserted, {updated_count} updated")
                    
        except Exception as e:
            logger.error(f"Error storing stock records: {e}")
            raise
        
        return inserted_count, updated_count
    
    def get_data_quality_metrics(self) -> Dict:
        """Get data quality metrics for monitoring"""
        metrics_sql = """
        SELECT 
            symbol,
            COUNT(*) as total_records,
            COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') as recent_records,
            AVG(close_price) as avg_close_price,
            MIN(timestamp) as earliest_timestamp,
            MAX(timestamp) as latest_timestamp
        FROM stock_data 
        GROUP BY symbol
        ORDER BY symbol;
        """
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(metrics_sql)
                    results = cursor.fetchall()
                    
                    metrics = {}
                    for row in results:
                        symbol, total, recent, avg_price, earliest, latest = row
                        metrics[symbol] = {
                            'total_records': total,
                            'recent_records': recent,
                            'avg_close_price': float(avg_price) if avg_price else 0,
                            'earliest_timestamp': earliest,
                            'latest_timestamp': latest
                        }
                    
                    return metrics
                    
        except Exception as e:
            logger.error(f"Error getting data quality metrics: {e}")
            return {}
    
    def process_symbol(self, symbol: str) -> Tuple[bool, int, int]:
        """Process a single stock symbol"""
        try:
            # Fetch data
            stock_data = self.fetch_stock_data(symbol)
            if not stock_data:
                logger.warning(f"No data received for {symbol}")
                return False, 0, 0
            
            # Parse data
            records = self.parse_stock_data(stock_data)
            if not records:
                logger.warning(f"No valid records parsed for {symbol}")
                return False, 0, 0
            
            # Store data
            inserted, updated = self.store_stock_records(records)
            logger.info(f"Successfully processed {symbol}: {inserted} inserted, {updated} updated")
            
            return True, inserted, updated
            
        except Exception as e:
            logger.error(f"Failed to process {symbol}: {e}")
            return False, 0, 0
    
    def run_pipeline(self, symbols: List[str] = None) -> Dict:
        """Run the complete data pipeline"""
        if symbols is None:
            symbols = self.default_symbols
        
        logger.info(f"Starting stock data pipeline for symbols: {symbols}")
        
        # Create table if needed
        if not self.create_table_if_not_exists():
            raise RuntimeError("Failed to create/verify database table")
        
        # Process each symbol
        results = {
            'successful_symbols': [],
            'failed_symbols': [],
            'total_inserted': 0,
            'total_updated': 0,
            'start_time': datetime.now(),
            'end_time': None
        }
        
        for i, symbol in enumerate(symbols):
            try:
                # Add delay between API calls (Alpha Vantage free tier: 5 calls per minute)
                if i > 0:
                    time.sleep(12)  # 12 seconds delay
                
                success, inserted, updated = self.process_symbol(symbol)
                
                if success:
                    results['successful_symbols'].append(symbol)
                    results['total_inserted'] += inserted
                    results['total_updated'] += updated
                else:
                    results['failed_symbols'].append(symbol)
                    
            except Exception as e:
                logger.error(f"Unexpected error processing {symbol}: {e}")
                results['failed_symbols'].append(symbol)
        
        results['end_time'] = datetime.now()
        
        # Log summary
        duration = (results['end_time'] - results['start_time']).total_seconds()
        logger.info(f"Pipeline completed in {duration:.2f} seconds")
        logger.info(f"Successful: {len(results['successful_symbols'])}, Failed: {len(results['failed_symbols'])}")
        logger.info(f"Total records: {results['total_inserted']} inserted, {results['total_updated']} updated")
        
        # Get and log data quality metrics
        metrics = self.get_data_quality_metrics()
        if metrics:
            logger.info("Data Quality Metrics:")
            for symbol, stats in metrics.items():
                logger.info(f"  {symbol}: {stats['total_records']} total records, "
                           f"{stats['recent_records']} recent, avg price: ${stats['avg_close_price']:.2f}")
        
        return results


def main():
    """Main function for command line execution"""
    parser = argparse.ArgumentParser(description='Stock Market Data Fetcher')
    parser.add_argument('--symbols', nargs='+', help='Stock symbols to fetch (e.g., AAPL GOOGL)')
    parser.add_argument('--single', type=str, help='Fetch data for a single symbol')
    parser.add_argument('--metrics', action='store_true', help='Show data quality metrics only')
    parser.add_argument('--create-table', action='store_true', help='Create table and exit')
    
    args = parser.parse_args()
    
    try:
        fetcher = StockDataFetcher()
        
        if args.create_table:
            if fetcher.create_table_if_not_exists():
                print("Table created successfully")
                return 0
            else:
                print("Failed to create table")
                return 1
        
        if args.metrics:
            metrics = fetcher.get_data_quality_metrics()
            if metrics:
                print("\nData Quality Metrics:")
                for symbol, stats in metrics.items():
                    print(f"{symbol}: {stats['total_records']} records, "
                          f"latest: {stats['latest_timestamp']}, "
                          f"avg price: ${stats['avg_close_price']:.2f}")
            else:
                print("No data found")
            return 0
        
        # Determine symbols to process
        symbols = None
        if args.single:
            symbols = [args.single.upper()]
        elif args.symbols:
            symbols = [s.upper() for s in args.symbols]
        
        # Run pipeline
        results = fetcher.run_pipeline(symbols)
        
        # Print results
        print(f"\nPipeline Results:")
        print(f"Successful symbols: {results['successful_symbols']}")
        print(f"Failed symbols: {results['failed_symbols']}")
        print(f"Records inserted: {results['total_inserted']}")
        print(f"Records updated: {results['total_updated']}")
        
        # Exit with error code if all symbols failed
        if len(results['successful_symbols']) == 0:
            return 1
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())