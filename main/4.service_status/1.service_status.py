import os
import requests
import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, DateTime, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, Timeout, RequestException

# Load environment variables from .env file
load_dotenv()

# Cloudera API credentials
CLOUDERA_URL = 'https://10.104.4.19:7183/api/v31/clusters/RTARF_CDP/services'
CLOUDERA_AUTH = (os.getenv('CLOUDERA_USER'), os.getenv('CLOUDERA_PASS'))

# Database credentials
DB_CONFIG = {
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME')
}

def create_db_connection():
    """Create a PostgreSQL connection using SQLAlchemy."""
    try:
        connection_uri = URL.create(
            drivername='postgresql+psycopg2',
            username=DB_CONFIG['username'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database']
        )
        engine = create_engine(connection_uri)
        print("✅ Database connection established.")
        return engine
    except SQLAlchemyError as e:
        print(f"❌ Database connection failed: {e}")
        return None

def create_table_if_not_exists(engine):
    """Create table if it does not exist."""
    metadata = MetaData()

    service_status_table = Table(
        "cloudera_service_status", metadata,
        Column("service_name", String, nullable=False),
        Column("health_status", String, nullable=False),
        Column("timestamp", DateTime, nullable=False)
    )

    try:
        metadata.create_all(engine)
        print("✅ Table checked/created successfully.")
    except SQLAlchemyError as e:
        print(f"❌ Error creating table: {e}")

def fetch_service_status():
    """Fetch service status from Cloudera API."""
    try:
        response = requests.get(CLOUDERA_URL, auth=CLOUDERA_AUTH, verify=False)
        response.raise_for_status()
    except ConnectionError:
        print("❌ Connection failed. Please check your network.")
        return []
    except Timeout:
        print("⏳ Request timed out. Please try again later.")
        return []
    except RequestException as e:
        print(f"❌ An error occurred: {e}")
        return []
    
    services = response.json().get('items', [])
    if not services:
        print("⚠️ No services found.")
        return []
    
    records = []
    for service in services:
        name = service['displayName']
        health_summary = service['healthSummary']

        records.append({
            "service_name": name,
            "health_status": health_summary
        })
    
    return records

def store_service_status(engine, data):
    """Store service status in PostgreSQL."""
    if not data:
        print("⚠️ No data to store.")
        return

    try:
        df = pd.DataFrame(data)
        df['timestamp'] = pd.Timestamp.utcnow()  # Add timestamp for tracking

        with engine.begin() as conn:
            df.to_sql('cloudera_service_status', conn, if_exists='append', index=False)
        
        print("✅ Data stored successfully in the database.")
    
    except SQLAlchemyError as e:
        print(f"❌ Failed to store data in the database: {e}")

def main():
    """Main function to fetch service status and store it in the database."""
    engine = create_db_connection()
    if not engine:
        return

    create_table_if_not_exists(engine)
    
    service_data = fetch_service_status()
    store_service_status(engine, service_data)

if __name__ == "__main__":
    main()
