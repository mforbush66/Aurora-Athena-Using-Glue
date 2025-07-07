# Employee Database Migration Plan

## Overview

This document outlines the plan for loading the sample employee database into the Aurora MySQL instance created by our ETL pipeline.

## Database Structure

The employee database consists of the following tables:
- `employees`: Basic employee information (ID, name, birth date, gender, hire date)
- `departments`: Department information (ID, name)
- `dept_manager`: Department manager assignments with time periods
- `dept_emp`: Department employee assignments with time periods
- `titles`: Employee job titles with time periods
- `salaries`: Employee salaries with time periods

And the following views:
- `dept_emp_latest_date`: Shows the latest department assignment dates for each employee
- `current_dept_emp`: Shows only the current department for each employee

## Source Data Files

- Schema: `/Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/employees.sql`
- Data dump files:
  - `load_departments.dump`: Department data
  - `load_dept_emp.dump`: Department-employee relationships
  - `load_dept_manager.dump`: Department manager assignments
  - `load_employees.dump`: Employee data
  - `load_salaries1.dump`, `load_salaries2.dump`, `load_salaries3.dump`: Salary data (split into multiple files)
  - `load_titles.dump`: Job titles data

## Database Connection Details

- Host: `aurora-mysql-cluster.cluster-c472k0sskdbg.us-east-1.rds.amazonaws.com`
- Username: Retrieved from parameters.json (`master_username`)
- Password: Retrieved from .env file (`AURORA_DB_PASSWORD`)
- Database: `employees` (will be created)
- Port: 3306

## Migration Process

### 1. Prerequisites

- Ensure the Aurora MySQL cluster is running and accessible
- Verify network connectivity from the environment where the migration will run
- Confirm MySQL client tools are available (mysql CLI or Python's pymysql)

### 2. Loading the Schema

```python
import os
import pymysql
from dotenv import load_dotenv
import json

# Load environment variables and configuration
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))
with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'parameters.json')) as f:
    params = json.load(f)

# Get Aurora connection details
aur_params = params.get('aurora', {})
host = f"aurora-mysql-cluster.cluster-c472k0sskdbg.us-east-1.rds.amazonaws.com"
user = aur_params.get('master_username', 'admin')
password = os.environ.get('AURORA_DB_PASSWORD')
port = aur_params.get('port', 3306)

# Read schema SQL file
schema_path = '/Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/employees.sql'
with open(schema_path, 'r') as f:
    schema_sql = f.read()

# Execute schema SQL
conn = pymysql.connect(host=host, user=user, password=password, port=port)
try:
    with conn.cursor() as cursor:
        # Split SQL into statements and execute each one
        for statement in schema_sql.split(';'):
            if statement.strip():
                cursor.execute(statement)
        conn.commit()
    print("Schema successfully loaded")
except Exception as e:
    print(f"Error loading schema: {e}")
finally:
    conn.close()
```

### 3. Loading the Data

```python
# Function to load data from dump files
def load_dump_file(dump_path, connection, table_name):
    print(f"Loading data for table {table_name}...")
    try:
        with open(dump_path, 'r') as f:
            dump_content = f.read()
        
        with connection.cursor() as cursor:
            cursor.execute(f"USE employees")
            cursor.execute(dump_content)
            connection.commit()
            
        # Verify data was loaded
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  Loaded {count} rows into {table_name}")
            
        return True
    except Exception as e:
        print(f"Error loading {table_name}: {e}")
        return False

# Connect to database
conn = pymysql.connect(host=host, user=user, password=password, port=port)

# Define dump files to load
dump_files = {
    'departments': '/Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/load_departments.dump',
    'employees': '/Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/load_employees.dump',
    'dept_manager': '/Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/load_dept_manager.dump',
    'dept_emp': '/Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/load_dept_emp.dump',
    'titles': '/Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/load_titles.dump'
}

# Load data for each table
try:
    for table, dump_file in dump_files.items():
        load_dump_file(dump_file, conn, table)
    
    # Handle salaries separately (multiple files)
    salary_files = [
        '/Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/load_salaries1.dump',
        '/Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/load_salaries2.dump',
        '/Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/load_salaries3.dump'
    ]
    
    for salary_file in salary_files:
        load_dump_file(salary_file, conn, 'salaries')
        
finally:
    conn.close()
```

### 4. Data Validation

```python
# Function to validate data
def validate_data():
    conn = pymysql.connect(host=host, user=user, password=password, port=port, db='employees')
    try:
        with conn.cursor() as cursor:
            # Check table counts
            tables = ['employees', 'departments', 'dept_manager', 'dept_emp', 'titles', 'salaries']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table}: {count} rows")
            
            # Run a sample query to verify relationships
            cursor.execute("""
                SELECT e.first_name, e.last_name, d.dept_name, t.title
                FROM employees e
                JOIN dept_emp de ON e.emp_no = de.emp_no
                JOIN departments d ON de.dept_no = d.dept_no
                JOIN titles t ON e.emp_no = t.emp_no
                WHERE de.to_date = '9999-01-01' AND t.to_date = '9999-01-01'
                LIMIT 5
            """)
            
            results = cursor.fetchall()
            print("\nSample employee data with department and title:")
            for row in results:
                print(f"  {row[0]} {row[1]} - {row[2]} ({row[3]})")
                
    except Exception as e:
        print(f"Validation error: {e}")
    finally:
        conn.close()

validate_data()
```

### 5. Alternative Approach: Using the MySQL CLI

If the Python approach encounters issues, we can use the MySQL command-line client:

```bash
# Load schema
mysql -h aurora-mysql-cluster.cluster-c472k0sskdbg.us-east-1.rds.amazonaws.com -u admin -p < /Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/employees.sql

# Load data (one example, repeat for each file)
mysql -h aurora-mysql-cluster.cluster-c472k0sskdbg.us-east-1.rds.amazonaws.com -u admin -p employees < /Users/mattforbush/Projects/AWS-DE_Academy/data/test_db/load_employees.dump
```

## Implementation Plan

1. Create a Python script `load_employee_db.py` implementing the approach described above
2. Test connectivity to Aurora MySQL from the development environment
3. Run the schema loading script and verify database creation
4. Load data tables one by one, starting with the tables without foreign key dependencies
5. Validate data by running sample queries
6. Document any issues encountered and how they were resolved

## Potential Issues and Mitigations

1. **Network Connectivity**: If the Aurora instance is not reachable, we may need to use AWS Systems Manager Session Manager or a jump host within the VPC.

2. **Data Format**: If the dump files have incompatibilities with Aurora MySQL, we may need to modify them or use an alternative import method.

3. **Performance**: Loading large data files may take time. Consider splitting the process and adding progress reporting.

4. **Foreign Key Constraints**: Ensure tables are loaded in the correct order to satisfy foreign key dependencies (employees and departments first, then junction tables).

## Success Criteria

- All tables and views from the employee database are created in Aurora MySQL
- All data is loaded correctly with the expected row counts
- Sample queries return expected results
- The entire migration process is documented and repeatable
