#!/usr/bin/env python3
"""
Setup Airflow connections for the stock data pipeline
Run this script after Airflow is started to configure database connections
"""

import os
import sys
from airflow import settings
from airflow.models import Connection
# from sqlalchemy.orm import sessionmaker  # Removed unused import

def create_connection(conn_id, conn_type, host, login, password, port, schema=None, extra=None):
    """Create or update an Airflow connection"""
    session = settings.Session()
    
    # Check if connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if existing_conn:
        print(f"Updating existing connection: {conn_id}")
        existing_conn.conn_type = conn_type
        existing_conn.host = host
        existing_conn.login = login
        existing_conn.password = password
        existing_conn.port = port
        existing_conn.schema = schema
        existing_conn.extra = extra
    else:
        print(f"Creating new connection: {conn_id}")
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=login,
            password=password,
            port=port,
            schema=schema,
            extra=extra
        )
        session.add(new_conn)
    
    session.commit()
    session.close()

def main():
    """Setup all required connections"""
    try:
        # Stock PostgreSQL connection
        create_connection(
            conn_id='stock_postgres',
            conn_type='postgres',
            host=os.getenv('STOCK_DB_HOST', 'stock-postgres'),
            login=os.getenv('STOCK_DB_USER', 'stockuser'),
            password=os.getenv('STOCK_DB_PASSWORD', 'stockpassword'),
            port=int(os.getenv('STOCK_DB_PORT', 5432)),
            schema=os.getenv('STOCK_DB_NAME', 'stockdb')
        )
        
        print("All connections created successfully!")
        return 0
        
    except Exception as e:
        print(f"Error setting up connections: {e}")
        return 1

if __name__ == '__main__':
    exit(main())