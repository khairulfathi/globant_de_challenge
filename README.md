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