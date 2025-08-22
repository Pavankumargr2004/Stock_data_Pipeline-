Dockerized Stock Data Pipeline with Airflow
A robust, scalable data pipeline that automatically fetches stock market data from Alpha Vantage API and stores it in PostgreSQL using Apache Airflow for orchestration.

🏗️ Architecture
The pipeline consists of the following components:

Apache Airflow: Orchestration and scheduling
PostgreSQL: Data storage (separate instances for Airflow metadata and stock data)
Redis: Message broker for Celery executor
Docker Compose: Container orchestration
Alpha Vantage API: Stock market data source
📋 Features
Automated Data Fetching: Hourly scheduled data retrieval from Alpha Vantage API
Robust Error Handling: Comprehensive retry logic and graceful failure management
Data Quality Validation: Automatic data quality checks and anomaly detection
Scalable Architecture: Designed to handle increased data volume and processing frequency
Security: Environment variable management for API keys and credentials
Monitoring: Detailed logging and data quality metrics
Fault Tolerance: Database connection pooling and transaction management
🚀 Quick Start
Prerequisites
Docker and Docker Compose installed
Alpha Vantage API key (free tier available at Alpha Vantage)
Setup Instructions
Clone or create the project structure:
bash
mkdir stock-pipeline && cd stock-pipeline
mkdir -p dags logs plugins scripts init-db
Create environment file:
bash
cp .env.example .env
# Edit .env and add your Alpha Vantage API key
Set Airflow user ID (Linux/macOS):
bash
echo -e "AIRFLOW_UID=$(id -u)" >> .env
Start the pipeline:
bash
docker-compose up -d
Initialize Airflow (first time only):
bash
# Wait for initialization to complete
docker-compose logs airflow-init
Setup Airflow connections:
bash
docker-compose exec airflow-webserver python /opt/airflow/scripts/setup_airflow_connections.py
Access the Airflow UI:
URL: http://localhost:8080
Username: airflow
Password: airflow
📁 Project Structure
stock-pipeline/
├── docker-compose.yml          # Main orchestration file
├── .env                        # Environment variables
├── requirements.txt            # Python dependencies
├── dags/
│   └── stock_data_pipeline.py  # Airflow DAG
├── scripts/
│   ├── stock_data_fetcher.py   # Standalone fetcher script
│   └── setup_airflow_connections.py  # Connection setup
├── init-db/
│   └── 01-init.sql            # Database initialization
                 # Custom Airflow plugins
🔧 Configuration
Environment Variables
Edit the .env file to configure the pipeline:

bash
# Alpha Vantage API Key (REQUIRED)
ALPHA_VANTAGE_API_KEY=your_api_key_here

# Database Configuration
STOCK_DB_HOST=stock-postgres
STOCK_DB_NAME=stockdb
STOCK_DB_USER=stockuser
STOCK_DB_PASSWORD=stockpassword
STOCK_DB_PORT=5432

# Airflow Configuration
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
Stock Symbols
The pipeline fetches data for the following symbols by default:

AAPL (Apple Inc.)
GOOGL (Alphabet Inc.)
MSFT (Microsoft Corporation)
AMZN (Amazon.com Inc.)
TSLA (Tesla Inc.)
To modify symbols, edit the STOCK_SYMBOLS list in dags/stock_data_pipeline.py.

Schedule Configuration
The pipeline runs hourly by default. To change the schedule, modify the schedule_interval in the DAG:

python
# In dags/stock_data_pipeline.py
schedule_interval='@hourly',  # Change to '@daily', '0 9 * * 1-5', etc.
📊 Database Schema
The pipeline creates the following table structure:

sql
CREATE TABLE stock_data (
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
Database Views
The initialization script also creates helpful views:

latest_stock_prices: Latest price for each symbol
daily_stock_summary: Daily aggregated statistics
🔍 Monitoring and Troubleshooting
Accessing Logs
bash
# View all services
docker-compose logs

# View specific service logs
docker-compose logs airflow-scheduler
docker-compose logs airflow-worker
docker-compose logs stock-postgres

# Follow logs in real-time
docker-compose logs -f airflow-scheduler
Database Access
Connect to the stock database:

bash
# Using docker exec
docker-compose exec stock-postgres psql -U stockuser -d stockdb

# Using external tools (host: localhost, port: 5433)
psql -h localhost -p 5433 -U stockuser -d stockdb
Useful SQL Queries
sql
-- Check recent data
SELECT symbol, COUNT(*) as records, MAX(timestamp) as latest
FROM stock_data 
WHERE created_at >= NOW() - INTERVAL '24 hours'
GROUP BY symbol;

-- View latest prices
SELECT * FROM latest_stock_prices;

-- Get statistics for a symbol
SELECT * FROM get_stock_stats('AAPL');
Standalone Script Usage
The fetcher can also be run independently:

bash
# Run for all default symbols
python scripts/stock_data_fetcher.py

# Run for specific symbols
python scripts/stock_data_fetcher.py --symbols AAPL MSFT

# Run for single symbol
python scripts/stock_data_fetcher.py --single GOOGL

# Show data quality metrics
python scripts/stock_data_fetcher.py --metrics

# Create table only
python scripts/stock_data_fetcher.py --create-table
🛠️ Customization
Adding New Data Sources
To add additional APIs or data sources:

Create a new fetcher function in the DAG
Add API configuration to environment variables
Extend the database schema if needed
Add new tasks to the DAG workflow
Scaling Considerations
For production deployment:

Database: Use managed PostgreSQL service or configure connection pooling
Airflow: Deploy on Kubernetes or use managed Airflow service
API Limits: Implement distributed fetching or upgrade to paid API tiers
Storage: Consider data partitioning for large datasets
Monitoring: Add alerting and monitoring systems
Error Handling
The pipeline includes multiple layers of error handling:

API Level: Retry logic with exponential backoff
Data Level: Validation and quality checks
Database Level: Transaction management and conflict resolution
Pipeline Level: Task-level retries and failure notifications
🔒 Security Best Practices
Store all credentials in environment variables
Use strong passwords for database accounts
Regularly rotate API keys
Enable SSL/TLS for database connections in production
Implement network segmentation in production deployments
📈 Performance Optimization
Database indexes are automatically created for optimal query performance
UPSERT operations prevent duplicate data
Connection pooling reduces database overhead
Configurable batch sizes for large datasets
🔄 Maintenance
Regular Tasks
Monitor API usage to avoid rate limits
Check data quality metrics regularly
Archive old data if storage becomes a concern
Update dependencies periodically
Backup database regularly
Upgrades
To upgrade Airflow version:

Update the image tag in docker-compose.yml
Check compatibility with existing DAGs
Test in development environment first
Follow Airflow upgrade documentation
🤝 Contributing
Follow PEP 8 coding standards
Add comprehensive error handling
Include logging for debugging
Update documentation for new features
Test changes thoroughly
📜 License
This project is provided as-is for educational and development purposes.

🆘 Support
For issues and questions:

Check the logs for error messages
Verify API key and database credentials
Ensure all services are running healthy
Check Alpha Vantage API status and limits
Review Airflow documentation for advanced configurations
Note: The Alpha Vantage free tier has rate limits (5 API calls per minute, 500 calls per day). For production use, consider upgrading to a paid plan or implementing additional data sources.

