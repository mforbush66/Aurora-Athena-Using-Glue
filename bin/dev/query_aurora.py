#!/usr/bin/env python3

import pymysql
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load configuration
project_dir = Path(__file__).resolve().parents[2]
env_path = project_dir / "bin" / ".env"
load_dotenv(env_path)

# Load parameters
params_path = project_dir / "bin" / "parameters.json"
with params_path.open("r", encoding="utf-8") as fh:
    params = json.load(fh)

# Connection parameters
host = "aurora-mysql-cluster.cluster-c472k0sskdbg.us-east-1.rds.amazonaws.com"
user = params["aurora"]["master_username"]
password = os.environ["AURORA_DB_PASSWORD"]
database = params["aurora"]["db_name"]
port = params["aurora"]["port"]

print(f"Connecting to {host} as {user}...")

employee_tables = ["employees", "departments", "dept_manager", "dept_emp", "titles", "salaries"]

try:
    # Connect to Aurora MySQL
    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        connect_timeout=10
    )
    
    print("Connected to Aurora MySQL")
    
    # List databases
    with conn.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        databases = [row[0] for row in cursor.fetchall()]
        print("\nAvailable databases:")
        for db in databases:
            print(f"- {db}")
    
    # List tables in current database
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"\nTables in '{database}' database:")
        for table in tables:
            print(f"- {table}")
    
    # If customers table exists, show sample data
    if 'customers' in tables:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM customers LIMIT 10")
            columns = [col[0] for col in cursor.description]
            print("\nSample data from 'customers' table:")
            print(f"{'=' * 60}")
            print(f"{columns[0]:<10} {columns[1]:<20} {columns[2]:<20} {columns[3]}")
            print(f"{'-' * 60}")
            for row in cursor.fetchall():
                print(f"{row[0]:<10} {row[1]:<20} {row[2]:<20} {row[3]}")
    
    # Check for employee tables
    for table in employee_tables:
        if table in tables:
            print(f"\nTable '{table}' already exists.")
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  > {count} rows")
    
    # Ask if user wants to create employee tables
    choice = input("\nDo you want to create the employee tables now? (yes/no): ")
    
    if choice.lower() in ["y", "yes"]:
        print("\nCreating employee tables...")
        
        test_db_path = Path(project_dir).parent / "data" / "test_db"
        print(f"Looking for employee data files in: {test_db_path}")
        
        # Create tables
        try:
            with conn.cursor() as cursor:
                # Create employees table
                print("Creating employees table...")
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    emp_no      INT             NOT NULL,
                    birth_date  DATE            NOT NULL,
                    first_name  VARCHAR(14)     NOT NULL,
                    last_name   VARCHAR(16)     NOT NULL,
                    gender      ENUM ('M','F')  NOT NULL,    
                    hire_date   DATE            NOT NULL,
                    PRIMARY KEY (emp_no)
                )
                """)
                
                # Create departments table
                print("Creating departments table...")
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS departments (
                    dept_no     CHAR(4)         NOT NULL,
                    dept_name   VARCHAR(40)     NOT NULL,
                    PRIMARY KEY (dept_no),
                    UNIQUE KEY (dept_name)
                )
                """)
                
                # Create dept_manager table
                print("Creating dept_manager table...")
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS dept_manager (
                    emp_no       INT             NOT NULL,
                    dept_no      CHAR(4)         NOT NULL,
                    from_date    DATE            NOT NULL,
                    to_date      DATE            NOT NULL,
                    PRIMARY KEY (emp_no,dept_no),
                    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
                    FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE
                )
                """)
                
                # Create dept_emp table
                print("Creating dept_emp table...")
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS dept_emp (
                    emp_no      INT             NOT NULL,
                    dept_no     CHAR(4)         NOT NULL,
                    from_date   DATE            NOT NULL,
                    to_date     DATE            NOT NULL,
                    PRIMARY KEY (emp_no,dept_no),
                    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
                    FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE
                )
                """)
                
                # Create titles table
                print("Creating titles table...")
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS titles (
                    emp_no      INT             NOT NULL,
                    title       VARCHAR(50)     NOT NULL,
                    from_date   DATE            NOT NULL,
                    to_date     DATE,
                    PRIMARY KEY (emp_no,title,from_date),
                    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE
                )
                """)
                
                # Create salaries table
                print("Creating salaries table...")
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS salaries (
                    emp_no      INT             NOT NULL,
                    salary      INT             NOT NULL,
                    from_date   DATE            NOT NULL,
                    to_date     DATE            NOT NULL,
                    PRIMARY KEY (emp_no,from_date),
                    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE
                )
                """)
                
                # Create views
                print("Creating views...")
                cursor.execute("""
                CREATE OR REPLACE VIEW dept_emp_latest_date AS
                    SELECT emp_no, MAX(from_date) AS from_date, MAX(to_date) AS to_date
                    FROM dept_emp
                    GROUP BY emp_no
                """)
                
                cursor.execute("""
                CREATE OR REPLACE VIEW current_dept_emp AS
                    SELECT l.emp_no, dept_no, l.from_date, l.to_date
                    FROM dept_emp d
                        INNER JOIN dept_emp_latest_date l
                        ON d.emp_no=l.emp_no AND d.from_date=l.from_date AND l.to_date = d.to_date
                """)
                
                conn.commit()
                print("Tables and views created successfully.")
            
            # Load data
            print("\nLoading data from dump files...")
            dump_files = {
                "departments": test_db_path / "load_departments.dump",
                "employees": test_db_path / "load_employees.dump",
                "dept_manager": test_db_path / "load_dept_manager.dump",
                "dept_emp": test_db_path / "load_dept_emp.dump",
                "titles": test_db_path / "load_titles.dump",
                "salaries1": test_db_path / "load_salaries1.dump",
                "salaries2": test_db_path / "load_salaries2.dump",
                "salaries3": test_db_path / "load_salaries3.dump"
            }
            
            # Check if files exist
            for name, path in dump_files.items():
                if path.exists():
                    print(f"Found {name} file: {path}")
                else:
                    print(f"ERROR: Missing file: {path}")
            
            # Define loading order to satisfy foreign key constraints
            loading_order = [
                ("departments", dump_files["departments"]),
                ("employees", dump_files["employees"]),
                ("dept_manager", dump_files["dept_manager"]),
                ("dept_emp", dump_files["dept_emp"]),
                ("titles", dump_files["titles"]),
            ]
            
            # Load each table
            for table_name, dump_file in loading_order:
                print(f"\nLoading {table_name} data...")
                try:
                    with open(dump_file, 'r') as f:
                        dump_content = f.read()
                        with conn.cursor() as c:
                            c.execute(dump_content)
                            conn.commit()
                    print(f"{table_name} data loaded successfully")
                except Exception as e:
                    print(f"Error loading {table_name}: {e}")
            
            # Handle salaries separately (multiple files)
            for i, salary_file in enumerate([dump_files["salaries1"], dump_files["salaries2"], dump_files["salaries3"]], 1):
                print(f"\nLoading salaries part {i}...")
                try:
                    with open(salary_file, 'r') as f:
                        dump_content = f.read()
                        with conn.cursor() as c:
                            c.execute(dump_content)
                            conn.commit()
                    print(f"Salaries part {i} loaded successfully")
                except Exception as e:
                    print(f"Error loading salaries part {i}: {e}")
            
            # Verify data load
            print("\nVerifying data load:")
            with conn.cursor() as c:
                for table in employee_tables:
                    c.execute(f"SELECT COUNT(*) FROM {table}")
                    count = c.fetchone()[0]
                    print(f"Table {table}: {count} rows")
                
                # Also verify the customer table is still intact
                c.execute("SELECT COUNT(*) FROM customers")
                count = c.fetchone()[0]
                print(f"Table customers: {count} rows")
            
        except Exception as e:
            print(f"Error creating tables or loading data: {e}")
    
    # Close connection
    conn.close()
    print("\nConnection closed")
    
except Exception as e:
    print(f"Error: {e}")
