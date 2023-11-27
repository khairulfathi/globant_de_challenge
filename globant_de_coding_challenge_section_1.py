from flask import Flask, request, jsonify
import csv
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

app = Flask(__name__)

# Define your Azure Blob Storage credentials
AZURE_STORAGE_CONNECTION_STRING = "your_connection_string"
AZURE_CONTAINER_NAME = "your_container_name"

# Define your database connection for PostgreSQL
POSTGRES_USERNAME = "your_username"
POSTGRES_PASSWORD = "your_password"
POSTGRES_HOST = "your_host"
POSTGRES_PORT = "your_port"
POSTGRES_DB = "your_database"

DATABASE_URL = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Define your models
class Department(Base):
    __tablename__ = 'departments'
    id = Column(Integer, primary_key=True) # Id of the department
    department_name = Column(String) # Name of the department

class Job(Base):
    __tablename__ = 'jobs'
    id = Column(Integer, primary_key=True) # Id of the job
    job = Column(String) # Name of the job

class Employee(Base):
    __tablename__ = 'employees'
    id = Column(Integer, primary_key=True) # Id of the employee
    name = Column(String) # Name of the employee
    hired_date = Column(DateTime) # Date when the employee was hired
    department_id = Column(Integer) # Id of the department where the employee works
    job_id = Column(Integer) # Id of the job of the employee

# Check if tables exist before creating them
existing_tables = engine.table_names()
if 'departments' not in existing_tables:
    Department.__table__.create(bind=engine)
if 'jobs' not in existing_tables:
    Job.__table__.create(bind=engine)
if 'employees' not in existing_tables:
    Employee.__table__.create(bind=engine)

# Function to read CSV file and insert data into the corresponding table
def insert_data(table, data):
    with engine.connect() as connection:
        connection.execute(table.insert(), data)

# Function to upload CSV file to Azure Blob Storage
def upload_to_azure_blob(file_content, file_name):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
    blob_client = container_client.get_blob_client(file_name)
    blob_client.upload_blob(file_content)

# Route to receive historical data from CSV files
@app.route('/upload', methods=['POST'])
def upload_csv():
    try:
        data = request.get_json()

        for table_name, csv_content in data.items():
            table = Base.metadata.tables.get(table_name)
            if table is not None:
                insert_data(table, csv_content)

        for file_name, file_content in request.files.items():
            upload_to_azure_blob(file_content.read(), file_name)

        return jsonify({"message": "Data uploaded successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
