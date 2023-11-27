# Data Engineering Challenge

## Section 1 for API
1. Receive historical data from CSV files
2. Upload the data into Postgres DB
3. Batch insert of data into respective tables

Language : Python
Libraries : flask, sqlalchemy (ORM)

### Assumptions:
1. CSV files columns and structure are fixed.
2. Each file is a full dataset including current and historical data not delta.

## Section 2 for SQL
1. Queries to be used to create endpoint that fulfill both requirements
2. Each query will have its own endpoint with `year` as parameter

### Assumptions:
1. Record with data quality issues have been omitted, these includes:
    - null or invalid hiring date
    - null department_id