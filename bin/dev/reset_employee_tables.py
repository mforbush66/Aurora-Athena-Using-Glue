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

# Define tables in reverse dependency order for dropping
employee_tables_drop_order = [
    "dept_emp_latest_date",  # view
    "current_dept_emp",     # view
    "salaries",
    "titles",
    "dept_emp",
    "dept_manager",
    "employees",
    "departments"
]

# Define tables in dependency order for creation
employee_tables_create_order = [
    "departments",
    "employees",
    "dept_manager",
    "dept_emp",
    "titles",
    "salaries"
]

try:
    # Connect to Aurora MySQL
    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        connect_timeout=60,
        read_timeout=300,
        write_timeout=300
    )
    
    print("Connected to Aurora MySQL successfully.")
    
    with conn.cursor() as c:
        # 1. Drop all employee tables and views
        print("\nDropping existing employee tables and views...")
        for table in employee_tables_drop_order:
            try:
                if table in ["dept_emp_latest_date", "current_dept_emp"]:
                    c.execute(f"DROP VIEW IF EXISTS {table}")
                else:
                    c.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"Dropped {table}")
            except Exception as e:
                print(f"Error dropping {table}: {e}")
        
        # 2. Create tables
        print("\nCreating employee tables...")
        
        # Create departments table
        print("Creating departments table...")
        c.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            dept_no     CHAR(4)         NOT NULL,
            dept_name   VARCHAR(40)     NOT NULL,
            PRIMARY KEY (dept_no),
            UNIQUE KEY (dept_name)
        )
        """)
        
        # Create employees table
        print("Creating employees table...")
        c.execute("""
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
        
        # Create dept_manager table
        print("Creating dept_manager table...")
        c.execute("""
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
        c.execute("""
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
        c.execute("""
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
        c.execute("""
        CREATE TABLE IF NOT EXISTS salaries (
            emp_no      INT             NOT NULL,
            salary      INT             NOT NULL,
            from_date   DATE            NOT NULL,
            to_date     DATE            NOT NULL,
            PRIMARY KEY (emp_no,from_date),
            FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE
        )
        """)
        conn.commit()
        
        # 3. Load data from dump files
        test_db_path = Path(project_dir).parent / "data" / "test_db"
        print(f"\nLooking for employee data in: {test_db_path}")
        
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
        
        # Load data in correct order
        print("\nLoading data...")
        
        # For small tables (load directly)
        small_tables = ["departments"]
        for table in small_tables:
            dump_file = dump_files[table]
            print(f"\nLoading {table} data...")
            try:
                with open(dump_file, 'r') as f:
                    dump_content = f.read()
                    c.execute(dump_content)
                    conn.commit()
                print(f"{table} data loaded successfully")
            except Exception as e:
                print(f"Error loading {table}: {e}")
                conn.rollback()
        
        # Load employees table first (required for foreign key constraints)
        print("\nLoading employees data...")
        try:
            with open(dump_files["employees"], 'r') as f:
                lines = []
                batch_count = 0
                insert_start = False
                success_count = 0
                
                for line in f:
                    line = line.strip()
                    
                    # Skip empty lines and comments
                    if not line or line.startswith('--'):
                        continue
                    
                    # Detect the start of an INSERT statement
                    if line.startswith('INSERT INTO'):
                        insert_start = True
                        lines = [line]
                    # Add to current batch if we're in an insert statement
                    elif insert_start and line.endswith(';'):
                        lines.append(line)
                        insert_start = False
                        
                        # Execute this batch
                        batch_count += 1
                        try:
                            # Print progress indicator
                            if batch_count % 100 == 0:
                                print(f"Processing employees batch {batch_count}...")
                            
                            c.execute('\n'.join(lines))
                            conn.commit()
                            success_count += 1
                        except Exception as e:
                            print(f"Error in employees batch {batch_count}: {str(e)[:100]}...")
                            conn.rollback()
                        
                        lines = []
                    elif insert_start:
                        lines.append(line)
                
                print(f"Completed employees loading: {success_count} successful batches")
        except Exception as e:
            print(f"Fatal error processing employees: {e}")
        
        # Now load other tables
        for table in ["dept_manager", "dept_emp", "titles"]:
            print(f"\nLoading {table} data...")
            try:
                with open(dump_files[table], 'r') as f:
                    lines = []
                    batch_count = 0
                    insert_start = False
                    success_count = 0
                    
                    for line in f:
                        line = line.strip()
                        
                        # Skip empty lines and comments
                        if not line or line.startswith('--'):
                            continue
                        
                        # Detect the start of an INSERT statement
                        if line.startswith('INSERT INTO'):
                            insert_start = True
                            lines = [line]
                        # Add to current batch if we're in an insert statement
                        elif insert_start and line.endswith(';'):
                            lines.append(line)
                            insert_start = False
                            
                            # Execute this batch
                            batch_count += 1
                            try:
                                # Print progress indicator
                                if batch_count % 100 == 0:
                                    print(f"Processing {table} batch {batch_count}...")
                                
                                c.execute('\n'.join(lines))
                                conn.commit()
                                success_count += 1
                            except Exception as e:
                                print(f"Error in {table} batch {batch_count}: {str(e)[:100]}...")
                                conn.rollback()
                            
                            lines = []
                        elif insert_start:
                            lines.append(line)
                    
                    print(f"Completed {table} loading: {success_count} successful batches")
            except Exception as e:
                print(f"Fatal error processing {table}: {e}")
        
        # Handle salaries separately (multiple files)
        for i, salary_key in enumerate(["salaries1", "salaries2", "salaries3"], 1):
            salary_file = dump_files[salary_key]
            print(f"\nLoading salaries part {i} from {salary_file}...")
            
            try:
                with open(salary_file, 'r') as f:
                    lines = []
                    batch_count = 0
                    insert_start = False
                    success_count = 0
                    
                    for line in f:
                        line = line.strip()
                        
                        # Skip empty lines and comments
                        if not line or line.startswith('--'):
                            continue
                        
                        # Detect the start of an INSERT statement
                        if line.startswith('INSERT INTO'):
                            insert_start = True
                            lines = [line]
                        # Add to current batch if we're in an insert statement
                        elif insert_start and line.endswith(';'):
                            lines.append(line)
                            insert_start = False
                            
                            # Execute this batch
                            batch_count += 1
                            try:
                                # Print progress indicator
                                if batch_count % 100 == 0:
                                    print(f"Processing salaries part {i} batch {batch_count}...")
                                
                                c.execute('\n'.join(lines))
                                conn.commit()
                                success_count += 1
                            except Exception as e:
                                print(f"Error in salaries part {i} batch {batch_count}: {str(e)[:100]}...")
                                conn.rollback()
                            
                            lines = []
                        elif insert_start:
                            lines.append(line)
                    
                    print(f"Completed salaries part {i} loading: {success_count} successful batches")
            except Exception as e:
                print(f"Fatal error processing salaries part {i}: {e}")
        
        # 4. Create views
        print("\nCreating views...")
        c.execute("""
        CREATE OR REPLACE VIEW dept_emp_latest_date AS
            SELECT emp_no, MAX(from_date) AS from_date, MAX(to_date) AS to_date
            FROM dept_emp
            GROUP BY emp_no
        """)
        
        c.execute("""
        CREATE OR REPLACE VIEW current_dept_emp AS
            SELECT l.emp_no, dept_no, l.from_date, l.to_date
            FROM dept_emp d
                INNER JOIN dept_emp_latest_date l
                ON d.emp_no=l.emp_no AND d.from_date=l.from_date AND l.to_date = d.to_date
        """)
        print("Views created successfully")
        
        # 5. Verify data load
        print("\nVerifying data load:")
        for table in employee_tables_create_order:
            c.execute(f"SELECT COUNT(*) FROM {table}")
            count = c.fetchone()[0]
            print(f"Table {table}: {count} rows")
        
        # Also verify the customer table is still intact
        c.execute("SELECT COUNT(*) FROM customers")
        count = c.fetchone()[0]
        print(f"Table customers: {count} rows")
    
    # Close connection
    conn.close()
    print("\nEmployee tables reset and data loaded successfully")
    
except Exception as e:
    print(f"Error: {e}")
