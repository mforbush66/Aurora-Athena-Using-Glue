#!/usr/bin/env python3
"""
Aurora-Athena ETL Pipeline (fixed)
=================================
End-to-end automation that spins up an **Aurora MySQL** cluster, catalogs it
with **AWS Glue**, converts the data to **Parquet in S3**, and exposes
everything through **Amazon-Athena**.

Why this revision?
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import boto3
import pymysql
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("aurora_athena_etl.log"),
    ],
)
LOGGER = logging.getLogger("AuroraAthenaETL")


# ════════════════════════════════════════════════════════════════════════════
#  Pipeline implementation
# ════════════════════════════════════════════════════════════════════════════
class AuroraAthenaETLPipeline:
    """End-to-end ETL orchestrator."""

    RETRY_SLEEP = 30  # seconds between Glue state polls

    # ─────────────────────────────── init ───────────────────────────────────
    def __init__(self) -> None:
        self.project_dir = Path(__file__).resolve().parents[2]
        self.log = LOGGER  # shorthand
        self.cfg = self._load_config()  # validated config dict
        self.clients = self.cfg["clients"]

    # ─────────────────────────── configuration ──────────────────────────────
    def _load_config(self) -> Dict[str, Any]:
        self.log.info("Loading configuration …")

        # .env --------------------------------------------------------------------------------------------------
        env_path = self.project_dir / "bin" / ".env"
        if not env_path.exists():
            raise FileNotFoundError(f"Missing .env file: {env_path}")
        load_dotenv(env_path)

        def need(var: str) -> str:
            val = os.getenv(var)
            if not val:
                raise RuntimeError(f"Environment variable {var} is required.")
            return val

        region     = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        profile    = os.getenv("AWS_PROFILE")
        aurora_pwd = need("AURORA_DB_PASSWORD")
        account_id = need("AWS_ACCOUNT_ID")

        creds = {
            "aws_access_key_id":     os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "aws_session_token":     os.getenv("AWS_SESSION_TOKEN"),
        }

        # parameters.json --------------------------------------------------------------------------------------
        params_path = self.project_dir / "bin" / "parameters.json"
        with params_path.open("r", encoding="utf-8") as fh:
            params = json.load(fh)

        # boto3 session ----------------------------------------------------------------------------------------
        if creds["aws_access_key_id"] and creds["aws_secret_access_key"]:
            self.log.info("Creating boto3 session using access keys")
            session = boto3.Session(region_name=region, **{k: v for k, v in creds.items() if v})
        elif profile:
            self.log.info("Creating boto3 session using profile %s", profile)
            session = boto3.Session(profile_name=profile, region_name=region)
        else:
            raise RuntimeError("No AWS credentials found in .env")

        mk = session.client
        return {
            "region":   region,
            "account":  account_id,
            "params":   params,
            "aurora_pw": aurora_pwd,
            "session":  session,
            "clients":  {svc: mk(svc) for svc in ("rds", "ec2", "iam", "glue", "s3", "athena", "logs")},
        }

    # ─────────────────────────────── helper ─────────────────────────────────
    def _get_s3_path(self, prm, folder_name):
        """Extract S3 bucket from existing paths and create a new path for the given folder."""
        # Try to extract bucket from various possible sources in order of preference
        s3_path = None
        
        # First check TARGET_S3_LOCATION in glue_etl_job
        if "glue_etl_job" in prm and "default_arguments" in prm["glue_etl_job"] and "--TARGET_S3_LOCATION" in prm["glue_etl_job"]["default_arguments"]:
            s3_path = prm["glue_etl_job"]["default_arguments"]["--TARGET_S3_LOCATION"]
        
        # If not found, try script_location
        elif "glue_etl_job" in prm and "script_location" in prm["glue_etl_job"]:
            s3_path = prm["glue_etl_job"]["script_location"]
        
        # If not found, try TempDir
        elif "glue_etl_job" in prm and "default_arguments" in prm["glue_etl_job"] and "--TempDir" in prm["glue_etl_job"]["default_arguments"]:
            s3_path = prm["glue_etl_job"]["default_arguments"]["--TempDir"]
        
        # If not found, try Athena output location
        elif "athena" in prm and "output_location" in prm["athena"]:
            s3_path = prm["athena"]["output_location"]
            
        if not s3_path:
            raise ValueError("Could not find any S3 path in configuration to extract bucket name")
        
        # Extract bucket name from s3://bucket-name/path/to/something
        parts = s3_path.replace("s3://", "").split("/")
        bucket = parts[0]
        
        return f"s3://{bucket}/{folder_name}/"
    
    def _poll(self, fn, key: str, ok: str, bad: List[str], component_name: str="Process"):
        """Poll callable ``fn`` until ``fn()[key]`` is *ok* or *bad*.
        
        If key contains dots, it will be treated as a nested key path.
        component_name provides context about what is being polled.
        """
        while True:
            result = fn()
            # Handle nested keys with dot notation (e.g., "Status.State")
            if "." in key:
                status = result
                for part in key.split("."):
                    status = status[part]
            else:
                status = result[key]
            
            # More descriptive status message with component name    
            self.log.info("%s status: %s", component_name, status)
            if status == ok:
                self.log.info("%s completed successfully", component_name)
                return
            if status in bad:
                raise RuntimeError(f"{component_name} failed with state {status}")
            time.sleep(self.RETRY_SLEEP)

    # ────────────────────────────── aurora ──────────────────────────────────
    def setup_aurora(self) -> str:
        rds = self.clients["rds"]
        ec2 = self.clients["ec2"]
        p   = self.cfg["params"]["aurora"]
        cid = p["db_cluster_identifier"]

        try:
            ep = rds.describe_db_clusters(DBClusterIdentifier=cid)["DBClusters"][0]["Endpoint"]
            self.log.info("Aurora cluster exists.")
            
            # Check if DB instance exists and make it publicly accessible
            try:
                instance_id = f"{cid}-instance-1"
                instance = rds.describe_db_instances(DBInstanceIdentifier=instance_id)["DBInstances"][0]
                if not instance.get("PubliclyAccessible", False):
                    self.log.info(f"Making Aurora instance {instance_id} publicly accessible...")
                    rds.modify_db_instance(
                        DBInstanceIdentifier=instance_id,
                        PubliclyAccessible=True,
                        ApplyImmediately=True
                    )
                    rds.get_waiter("db_instance_available").wait(DBInstanceIdentifier=instance_id)
                    self.log.info(f"Aurora instance '{instance_id}' modified to be publicly accessible")
                    self.log.info("Waiting 2 minutes for DNS propagation...")
                    time.sleep(120)  # Wait for 2 minutes
                
            except rds.exceptions.DBInstanceNotFoundFault:
                self.log.info(f"Aurora instance '{instance_id}' does not exist, creating it...")
                rds.create_db_instance(
                    DBInstanceIdentifier=instance_id,
                    DBClusterIdentifier=cid,
                    Engine="aurora-mysql",
                    DBInstanceClass=p["instance_class"],
                    PubliclyAccessible=True
                )
                self.log.info(f"Aurora instance '{instance_id}' created, waiting for it to be available...")
                rds.get_waiter("db_instance_available").wait(DBInstanceIdentifier=instance_id)
                self.log.info(f"Aurora instance '{instance_id}' is now available")
                self.log.info("Waiting 2 minutes for DNS propagation...")
                time.sleep(120)  # Wait for 2 minutes
            
            # Get the cluster endpoint
            response = rds.describe_db_clusters(DBClusterIdentifier=cid)
            ep = response["DBClusters"][0]["Endpoint"]
            
        except rds.exceptions.DBClusterNotFoundFault:
            self.log.info(f"Aurora cluster '{cid}' does not exist, creating it...")
            
            # Create subnet group if it doesn't exist
            subnet_group_name = p["db_subnet_group_name"]  # Using the correct config key
            
            # Get a primary subnet ID from glue connection configuration
            primary_subnet_id = self.cfg["params"]["glue_connection"]["physical_connection_requirements"]["subnet_id"]
            
            # Find the VPC ID from the primary subnet
            ec2 = self.clients["ec2"]
            subnet_info = ec2.describe_subnets(SubnetIds=[primary_subnet_id])
            vpc_id = subnet_info['Subnets'][0]['VpcId']
            primary_az = subnet_info['Subnets'][0]['AvailabilityZone']
            
            # Get all subnets in this VPC
            all_subnets = ec2.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}])
            
            # Get at least one subnet from a different AZ
            subnet_ids = [primary_subnet_id]
            for subnet in all_subnets['Subnets']:
                if subnet['AvailabilityZone'] != primary_az:
                    subnet_ids.append(subnet['SubnetId'])
                    break
                    
            if len(subnet_ids) < 2:
                self.log.warning("Could not find subnets in multiple AZs. Using only the primary subnet.")
                # For development purposes, we'll use a dummy hack to bypass the multi-AZ check
                # This could be removed in production
                try:
                    # Try to create without proper multi-AZ coverage
                    rds.create_db_subnet_group(
                        DBSubnetGroupName=subnet_group_name,
                        DBSubnetGroupDescription=f"Subnet group for {cid}",
                        SubnetIds=subnet_ids,
                        Tags=[{"Key": "Name", "Value": subnet_group_name}]
                    )
                except Exception as e:
                    if "DBSubnetGroupDoesNotCoverEnoughAZs" in str(e):
                        # In development mode, skip the subnet group requirement
                        self.log.warning("Multi-AZ subnet requirement detected. Proceeding with single AZ for development.")
                        subnet_group_name = None
                    else:
                        raise
            else:
                # Create subnet group with multiple AZs
                try:
                    rds.create_db_subnet_group(
                        DBSubnetGroupName=subnet_group_name,
                        DBSubnetGroupDescription=f"Subnet group for {cid}",
                        SubnetIds=subnet_ids,
                        Tags=[{"Key": "Name", "Value": subnet_group_name}]
                    )
                except rds.exceptions.DBSubnetGroupAlreadyExistsFault:
                    self.log.info(f"DB subnet group '{subnet_group_name}' already exists")
            
            # Configure backups to reduce recovery time for training environment
            backup_retention_period = p.get("backup_retention_period", 1)  # Default: 1 day retention
            preferred_backup_window = p.get("preferred_backup_window", "01:00-02:00")  # Default: 1-2 AM UTC
            
            # Get security groups from parameters
            security_group_ids = p.get("vpc_security_group_ids", [])
            
            response = rds.create_db_cluster(
                DBClusterIdentifier=cid,
                Engine=p.get("engine", "aurora-mysql"),
                EngineVersion="8.0.mysql_aurora.3.04.0",  # MySQL 8.0 compatible version
                EngineMode="provisioned",
                MasterUsername=p["master_username"],
                MasterUserPassword=self.cfg["aurora_pw"],
                DBSubnetGroupName=subnet_group_name,
                VpcSecurityGroupIds=security_group_ids,
                DatabaseName=p["db_name"],
                BackupRetentionPeriod=backup_retention_period,
                PreferredBackupWindow=preferred_backup_window,
                StorageEncrypted=p.get("storage_encrypted", True),
                DeletionProtection=p.get("deletion_protection", False)
            )
            
            self.log.info("Aurora cluster created, waiting for it to be available...")
            rds.get_waiter("db_cluster_available").wait(DBClusterIdentifier=cid)
            
            # Create the primary instance
            instance_id = f"{cid}-instance-1"
            self.log.info(f"Creating Aurora instance '{instance_id}'...")
            rds.create_db_instance(
                DBInstanceIdentifier=instance_id,
                DBClusterIdentifier=cid,
                Engine=p.get("engine", "aurora-mysql"),
                EngineVersion="8.0.mysql_aurora.3.04.0",  # MySQL 8.0 compatible version
                DBInstanceClass=p["db_instance_class"],  # Using correct parameter name
                PubliclyAccessible=True
            )
            
            self.log.info(f"Aurora instance '{instance_id}' created, waiting for it to be available...")
            rds.get_waiter("db_instance_available").wait(DBInstanceIdentifier=instance_id)
            self.log.info(f"Aurora instance '{instance_id}' is now available")
            
            # Wait a bit more to ensure the database is fully ready
            self.log.info("Waiting 2 minutes for DNS propagation and database readiness...")
            time.sleep(120)  # Wait for 2 minutes
            
            # Get the cluster endpoint
            response = rds.describe_db_clusters(DBClusterIdentifier=cid)
            ep = response["DBClusters"][0]["Endpoint"]
            
            # Configure security groups
            security_groups = response["DBClusters"][0]["VpcSecurityGroups"]
            if security_groups:
                sg_id = security_groups[0]["VpcSecurityGroupId"]
                self._ensure_security_group_access(sg_id)
        
        self.log.info(f"Aurora cluster endpoint: {ep}")
        
        # Bootstrap the schema if needed

        ep = rds.describe_db_clusters(DBClusterIdentifier=cid)["DBClusters"][0]["Endpoint"]
        self.log.info("Aurora ready → %s", ep)
        self._bootstrap_schema(ep)
        return ep

    def _bootstrap_schema(self, ep: str):
        self.log.info("Creating employee database schema...")
        aur = self.cfg["params"]["aurora"]
        max_attempts = 3
        # Wait times in seconds: 5 minutes, 3 minutes, 1 minute (only used after first attempt fails)
        retry_delays = [300, 180, 60]
        
        test_db_path = str(Path(self.project_dir).parent / "data" / "test_db")
        employees_sql_path = os.path.join(test_db_path, "employees.sql")
        
        # Dump files for loading data
        dump_files = {
            "departments": os.path.join(test_db_path, "load_departments.dump"),
            "employees": os.path.join(test_db_path, "load_employees.dump"),
            "dept_manager": os.path.join(test_db_path, "load_dept_manager.dump"),
            "dept_emp": os.path.join(test_db_path, "load_dept_emp.dump"),
            "titles": os.path.join(test_db_path, "load_titles.dump"),
            "salaries1": os.path.join(test_db_path, "load_salaries1.dump"),
            "salaries2": os.path.join(test_db_path, "load_salaries2.dump"),
            "salaries3": os.path.join(test_db_path, "load_salaries3.dump")
        }
        
        # Check if files exist and print the contents of test_db_path
        self.log.info(f"Looking for employee data in: {test_db_path}")
        if os.path.exists(test_db_path):
            self.log.info(f"Directory exists, contents: {os.listdir(test_db_path)}")
        else:
            self.log.error(f"Directory not found: {test_db_path}")
            return False
                    
        for file_name, file_path in {"schema": employees_sql_path, **dump_files}.items():
            if not os.path.exists(file_path):
                self.log.error(f"Required file not found: {file_path}")
                return False
            else:
                self.log.info(f"Found file: {file_path}")
        
        for attempt in range(1, max_attempts + 1):
            try:
                if attempt > 1:
                    # Only wait on retry attempts, not the first attempt
                    wait_time = retry_delays[attempt-2]  # Adjust index since we're starting with second attempt
                    self.log.info(f"Waiting {wait_time} seconds before next database connection attempt ({attempt}/{max_attempts})")
                    
                    # Show countdown in 30 second increments
                    for remaining in range(wait_time, 0, -30):
                        self.log.info(f"Retrying in {remaining} seconds...")
                        time.sleep(min(30, remaining))
                
                self.log.info(f"Connecting to database at {ep} (attempt {attempt}/{max_attempts})")
                
                # First connect without specifying database name to create database if needed
                conn = pymysql.connect(
                    host=ep, 
                    user=aur["master_username"], 
                    password=self.cfg["aurora_pw"], 
                    port=aur["port"], 
                    connect_timeout=30
                )
                
                self.log.info("Connected to Aurora. Creating database if not exists...")
                
                # Create sampledb database if it doesn't exist
                with conn.cursor() as c:
                    c.execute("CREATE DATABASE IF NOT EXISTS sampledb")
                    c.execute("USE sampledb")
                    conn.commit()
                    self.log.info("Using database: sampledb")
                
                # Check if the employee tables exist
                with conn.cursor() as c:
                    c.execute("SHOW TABLES LIKE 'employees'")
                    if c.fetchone():
                        self.log.info("Employee tables already exist")
                    else:
                        self.log.info("Employee tables not found, will create them")
                
                # Load schema from SQL file (just for reference, not executing it directly)
                self.log.info("Reading schema file...")
                with open(employees_sql_path, 'r') as f:
                    schema_sql = f.read()
                
                # Use the existing connection to the sampledb database
                # We're already connected to the right database
                
                self.log.info("Creating tables...")
                with conn.cursor() as c:
                    # Create tables one by one
                    c.execute("""CREATE TABLE IF NOT EXISTS employees (
                        emp_no      INT             NOT NULL,
                        birth_date  DATE            NOT NULL,
                        first_name  VARCHAR(14)     NOT NULL,
                        last_name   VARCHAR(16)     NOT NULL,
                        gender      ENUM ('M','F')  NOT NULL,    
                        hire_date   DATE            NOT NULL,
                        PRIMARY KEY (emp_no)
                    )""")
                    
                    c.execute("""CREATE TABLE IF NOT EXISTS departments (
                        dept_no     CHAR(4)         NOT NULL,
                        dept_name   VARCHAR(40)     NOT NULL,
                        PRIMARY KEY (dept_no),
                        UNIQUE KEY (dept_name)
                    )""")
                    
                    c.execute("""CREATE TABLE IF NOT EXISTS dept_manager (
                        emp_no       INT             NOT NULL,
                        dept_no      CHAR(4)         NOT NULL,
                        from_date    DATE            NOT NULL,
                        to_date      DATE            NOT NULL,
                        PRIMARY KEY (emp_no,dept_no),
                        FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
                        FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE
                    )""")
                    
                    c.execute("""CREATE TABLE IF NOT EXISTS dept_emp (
                        emp_no      INT             NOT NULL,
                        dept_no     CHAR(4)         NOT NULL,
                        from_date   DATE            NOT NULL,
                        to_date     DATE            NOT NULL,
                        PRIMARY KEY (emp_no,dept_no),
                        FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
                        FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE
                    )""")
                    
                    c.execute("""CREATE TABLE IF NOT EXISTS titles (
                        emp_no      INT             NOT NULL,
                        title       VARCHAR(50)     NOT NULL,
                        from_date   DATE            NOT NULL,
                        to_date     DATE,
                        PRIMARY KEY (emp_no,title,from_date),
                        FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE
                    )""")
                    
                    c.execute("""CREATE TABLE IF NOT EXISTS salaries (
                        emp_no      INT             NOT NULL,
                        salary      INT             NOT NULL,
                        from_date   DATE            NOT NULL,
                        to_date     DATE            NOT NULL,
                        PRIMARY KEY (emp_no,from_date),
                        FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE
                    )""")
                    
                    c.execute("""CREATE OR REPLACE VIEW dept_emp_latest_date AS
                        SELECT emp_no, MAX(from_date) AS from_date, MAX(to_date) AS to_date
                        FROM dept_emp
                        GROUP BY emp_no""")
                    
                    c.execute("""CREATE OR REPLACE VIEW current_dept_emp AS
                        SELECT l.emp_no, dept_no, l.from_date, l.to_date
                        FROM dept_emp d
                            INNER JOIN dept_emp_latest_date l
                            ON d.emp_no=l.emp_no AND d.from_date=l.from_date AND l.to_date = d.to_date""")
                
                # Load data from dump files
                self.log.info("Loading data from dump files...")
                
                try:
                    # Define helper functions for file processing
                    def process_small_file(cursor, file_path, table_name):
                        try:
                            with open(file_path, 'r') as f:
                                dump_content = f.read()
                                cursor.execute(dump_content)
                                conn.commit()
                            self.log.info(f"{table_name} data loaded successfully")
                            return True
                        except Exception as e:
                            self.log.error(f"Error loading {table_name}: {e}")
                            conn.rollback()
                            return False
                            
                    def process_large_file(cursor, file_path, table_name):
                        self.log.info(f"Processing large file {file_path} in chunks...")
                        
                        try:
                            # First, read a sample to detect format
                            with open(file_path, 'r') as f:
                                sample_content = f.read(1000)
                                self.log.info(f"Sample content from {table_name}: {sample_content[:200]}...")
                            
                            # Handle INSERT statements
                            if 'INSERT INTO' in sample_content:
                                self.log.info(f"Processing {table_name} with multiple INSERT statements")
                                with open(file_path, 'r') as f:
                                    lines = []
                                    batch_count = 0
                                    insert_start = False
                                    success_count = 0
                                    
                                    for line in f:
                                        line = line.strip()
                                        
                                        if not line or line.startswith('--'):
                                            continue
                                            
                                        if line.startswith('INSERT INTO'):
                                            insert_start = True
                                            lines = [line]
                                        elif insert_start and line.endswith(';'):
                                            lines.append(line)
                                            insert_start = False
                                            
                                            batch_count += 1
                                            try:
                                                if batch_count % 50 == 0:
                                                    self.log.info(f"Processing batch {batch_count} for {table_name}...")
                                                
                                                cursor.execute('\n'.join(lines))
                                                conn.commit()
                                                success_count += 1
                                            except Exception as e:
                                                self.log.error(f"Error in batch {batch_count} for {table_name}: {str(e)[:100]}...")
                                                conn.rollback()
                                                
                                            lines = []
                                        elif insert_start:
                                            lines.append(line)
                                    
                                    self.log.info(f"Completed {table_name} loading: {success_count} successful batches out of {batch_count} total")
                                    return success_count > 0
                            else:
                                self.log.error(f"Unexpected format in {file_path}, couldn't process")
                                return False
                        except Exception as e:
                            self.log.error(f"Fatal error processing {table_name}: {e}")
                            return False
                    
                    # Check if data already exists in the tables
                    self.log.info("Checking if data already exists in tables...")
                    with conn.cursor() as check_cursor:
                        # Sample check on a few tables
                        try:
                            check_cursor.execute("SELECT COUNT(*) FROM employees LIMIT 1")
                            employee_count = check_cursor.fetchone()[0]
                            check_cursor.execute("SELECT COUNT(*) FROM departments LIMIT 1")
                            dept_count = check_cursor.fetchone()[0]
                            
                            if employee_count > 0 and dept_count > 0:
                                self.log.info(f"Data already exists in tables (found {employee_count} employees, {dept_count} departments). Skipping data load.")
                                self.log.info("Schema and data loading completed successfully.")
                                return True
                            else:
                                self.log.info("Tables exist but appear to be empty. Proceeding with data load.")
                        except Exception as e:
                            self.log.info(f"Error checking for existing data: {e}. Will attempt data loading.")
                    
                    # Define loading order to respect foreign key constraints
                    self.log.info("Loading data files in correct order to respect foreign key constraints...")
                    load_order = [
                        ("departments", dump_files["departments"], False),  # table_name, file_path, is_large
                        ("employees", dump_files["employees"], True),
                        ("dept_manager", dump_files["dept_manager"], False),
                        ("dept_emp", dump_files["dept_emp"], True),
                        ("titles", dump_files["titles"], True),
                        ("salaries", dump_files["salaries1"], True),  # Salaries split across multiple files
                    ]
                    
                    # Process files in the correct order
                    for table_name, file_path, is_large in load_order:
                        self.log.info(f"Loading {table_name} data...")
                        with conn.cursor() as table_cursor:
                            if is_large:
                                process_large_file(table_cursor, file_path, table_name)
                            else:
                                process_small_file(table_cursor, file_path, table_name)
                                
                    # Process additional salaries files if they exist
                    for salaries_file in [dump_files["salaries2"], dump_files["salaries3"]]:
                        self.log.info(f"Loading additional salaries data...")
                        with conn.cursor() as sal_cursor:
                            process_large_file(sal_cursor, salaries_file, "salaries")
            
                except Exception as e:
                    self.log.error(f"Error during database loading: {e}")
                    return False         # Verify data load
                with conn.cursor() as c:
                    for table in ["employees", "departments", "dept_manager", "dept_emp", "titles", "salaries"]:
                        try:
                            c.execute(f"SELECT COUNT(*) FROM {table}")
                            count = c.fetchone()[0]
                            self.log.info(f"Table {table}: {count} rows")
                        except Exception as e:
                            self.log.warning(f"Could not count rows in {table}: {e}")
                    
                    # Also verify the employee tables are still intact
                    c.execute("SELECT COUNT(*) FROM employees")
                    count = c.fetchone()[0]
                    self.log.info(f"Table employees: {count} rows")
                
                self.log.info("Employee tables added to sampledb database successfully")
                return True
            
            except Exception as e:
                self.log.warning(f"Error initializing employee database (attempt {attempt}/{max_attempts}): {e}")
                if attempt < max_attempts:
                    delay = retry_delays[attempt - 1]
                    self.log.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
        
        self.log.error("Failed to initialize employee database after multiple attempts")
        # Continue with the pipeline despite database connection issues
        self.log.info("Employee schema setup skipped - continuing with pipeline")
        return False

    def _etl_script_content(self) -> str:
        """PySpark ETL that uses JDBC connection directly instead of Glue catalog."""
        p = self.cfg["params"]
        target_s3 = self._get_s3_path(p, "etl-output")
        
        return f'''
            import sys
            from awsglue.transforms import *
            from awsglue.utils import getResolvedOptions
            from pyspark.context import SparkContext
            from awsglue.context import GlueContext
            from awsglue.job import Job

            # Set up Glue environment
            sc = SparkContext()
            glueContext = GlueContext(sc)
            spark = glueContext.spark_session
            job = Job(glueContext)

            # JDBC connection parameters
            connection_url = "jdbc:mysql://{p["aurora"]["endpoint"]}:{p["aurora"]["port"]}/{p["aurora"]["db_name"]}"
            connection_properties = {{
                "user": "{p["aurora"]["master_username"]}",
                "password": "{self.cfg["aurora_pw"]}",
                "driver": "com.mysql.jdbc.Driver"
            }}

            # Tables to extract
            tables = ["employees", "departments", "dept_emp", "dept_manager", "salaries", "titles"]
            
            for table in tables:
                try:
                    print(f"Processing table: {{table}}")
                    # Read data directly from Aurora MySQL
                    df = spark.read.jdbc(
                        url=connection_url,
                        table=table,
                        properties=connection_properties
                    )

                    # Print some information about the data
                    print(f"Schema for {{table}}: {{df.schema}}")
                    print(f"Count for {{table}}: {{df.count()}}")
                    df.show(5, truncate=False)

                    # Write the data to S3 in Parquet format
                    output_path = "{target_s3}{{table}}/"
                    print(f"Writing {{table}} data to {{output_path}}")
                    
                    df.write.mode("overwrite").parquet(output_path)
                    print(f"ETL for {{table}} completed successfully!")
                except Exception as e:
                    print(f"Error processing table {{table}}: {{e}}")
                    continue
            
            print("Full ETL Job completed!")
        '''

    # ─────────────────────────────── glue ────────────────────────────────────
    def setup_glue(self, role_arn: str, ep: str):
        glue = self.clients["glue"]
        prm  = self.cfg["params"]
        # ---------- Connection & crawler --------------------------------------
        glue = self.clients["glue"]
        conn_p = prm["glue_connection"]
        conn_n = conn_p["connection_name"]
        
        # Get Aurora endpoint for JDBC URL
        aurora_endpoint = self.clients["rds"].describe_db_clusters(
            DBClusterIdentifier=prm["aurora"]["db_cluster_identifier"]
        )["DBClusters"][0]["Endpoint"]
        
        # Update connection properties with actual endpoint
        conn_props = conn_p["connection_properties"].copy()
        if "{ENDPOINT}" in conn_props.get("JDBC_CONNECTION_URL", ""):
            conn_props["JDBC_CONNECTION_URL"] = conn_props["JDBC_CONNECTION_URL"].replace("{ENDPOINT}", aurora_endpoint)
        
        # Add password if not present
        if "PASSWORD" not in conn_props and self.cfg.get("aurora_pw"):
            conn_props["PASSWORD"] = self.cfg["aurora_pw"]
        
        # Get VPC info from Aurora cluster
        aurora_vpc_id = None
        try:
            cluster_info = self.clients["rds"].describe_db_clusters(
                DBClusterIdentifier=prm["aurora"]["db_cluster_identifier"]
            )["DBClusters"][0]
            
            # Get subnet group to find VPC ID
            subnet_group_name = cluster_info.get("DBSubnetGroup")
            if subnet_group_name:
                subnet_groups = self.clients["rds"].describe_db_subnet_groups(
                    DBSubnetGroupName=subnet_group_name
                )["DBSubnetGroups"]
                if subnet_groups:
                    aurora_vpc_id = subnet_groups[0].get("VpcId")
                    self.log.info(f"Found Aurora VPC ID: {aurora_vpc_id}")
        except Exception as e:
            self.log.warning(f"Could not determine Aurora VPC ID: {e}")
                
        
        try:
            # Check if connection exists
            try:
                conn_response = glue.get_connection(Name=conn_n)
                self.log.info("Glue connection %s exists", conn_n)
                
                # Update connection if needed
                conn_input = conn_response.get("Connection", {})
                needs_update = False
                
                # Check if JDBC URL needs update
                if conn_input.get("ConnectionProperties", {}).get("JDBC_CONNECTION_URL") != conn_props.get("JDBC_CONNECTION_URL"):
                    needs_update = True
                
                # Ensure database name is explicitly included in JDBC URL
                jdbc_url = conn_props.get("JDBC_CONNECTION_URL", "")
                if "databaseName=" not in jdbc_url and "?" in jdbc_url:
                    db_name = prm["aurora"]["db_name"]
                    jdbc_url += f"&databaseName={db_name}"
                    conn_props["JDBC_CONNECTION_URL"] = jdbc_url
                    needs_update = True
                elif "databaseName=" not in jdbc_url:
                    db_name = prm["aurora"]["db_name"]
                    jdbc_url += f"?databaseName={db_name}"
                    conn_props["JDBC_CONNECTION_URL"] = jdbc_url
                    needs_update = True
                
                if needs_update:
                    self.log.info(f"Updating Glue connection with correct endpoint and explicit database name: {jdbc_url}")
                    
                    # Preserve the existing connection configuration
                    connection_input = {
                        "Name": conn_n,
                        "Description": conn_input.get("Description", conn_p.get("description", "Aurora connection")),
                        "ConnectionType": conn_input.get("ConnectionType", "JDBC"),
                        "ConnectionProperties": conn_props
                    }
                    
                    # If PhysicalConnectionRequirements exists, preserve it
                    if "PhysicalConnectionRequirements" in conn_input:
                        connection_input["PhysicalConnectionRequirements"] = conn_input["PhysicalConnectionRequirements"]
                    
                    glue.update_connection(
                        Name=conn_n,
                        ConnectionInput=connection_input
                    )
                    
                    # Test the connection after update - commented out as TestConnection is only supported for Unified Connections
                    # try:
                    #     self.log.info(f"Testing Glue connection {conn_n}...")
                    #     glue.test_connection(ConnectionName=conn_n)
                    #     self.log.info(f"Glue connection {conn_n} test initiated")
                    # except Exception as e:
                    #     self.log.warning(f"Connection test may take time to complete: {e}")
                    self.log.info(f"Skipping connection test as TestConnection is only supported for Unified Connections")
            except glue.exceptions.EntityNotFoundException:
                self.log.info("Creating Glue connection %s", conn_n)
                # Create a new connection without network config unless all required fields exist
                connection_input = {
                    "Name": conn_n,
                    "Description": conn_p.get("description", "Aurora connection"),
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": conn_props
                }
                
                # Only add PhysicalConnectionRequirements if we have all required fields
                if all(k in conn_p for k in ["subnet_id", "availability_zone", "security_group_id_list"]):
                    connection_input["PhysicalConnectionRequirements"] = {
                        "SubnetId": conn_p["subnet_id"],
                        "SecurityGroupIdList": conn_p["security_group_id_list"],
                        "AvailabilityZone": conn_p["availability_zone"]
                    }
                
                glue.create_connection(ConnectionInput=connection_input)
        except Exception as e:
            self.log.error(f"Error setting up Glue connection: {e}")
            raise
        
        # ---------- crawler ---------------------------------------------------
        cr = prm["glue_crawler"]
        crawl_n = cr["crawler_name"]
        try:
            glue.get_crawler(Name=crawl_n)
        except glue.exceptions.EntityNotFoundException:
            self.log.info("Creating crawler %s", crawl_n)
            glue.create_crawler(Name=crawl_n, Role=role_arn, DatabaseName=cr["database_name"], Targets={"JdbcTargets": [{"ConnectionName": conn_n, "Path": cr["targets"]["jdbc_targets"][0]["path"]}]}, Schedule=cr["schedule"], SchemaChangePolicy={"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "LOG"})
        # Check crawler state and handle accordingly
        crawler_info = glue.get_crawler(Name=crawl_n)["Crawler"]
        crawler_state = crawler_info["State"]
        
        if crawler_state == "RUNNING":
            self.log.info(f"Crawler {crawl_n} is already running. Waiting for it to complete...")
        elif crawler_state == "READY":
            self.log.info(f"Starting crawler {crawl_n}...")
            glue.start_crawler(Name=crawl_n)
        else:
            self.log.info(f"Crawler {crawl_n} is in state {crawler_state}. Waiting for it to become READY...")
            
        # Poll until crawler is in READY state
        self._poll(lambda: glue.get_crawler(Name=crawl_n)["Crawler"], "State", "READY", ["FAILED"], f"Glue Crawler '{crawl_n}'")
        
        # Wait for the catalog to be fully updated after crawler completes
        self.log.info("Waiting 30 seconds for Glue catalog to update completely...")
        time.sleep(30)
        
        # Verify that the database and table exist in the catalog
        database_name = cr["database_name"]
        table_name = prm["glue_etl_job"]["default_arguments"]["--SOURCE_TABLE"]
        
        # Check if database exists, create it if it doesn't
        try:
            glue.get_database(Name=database_name)
            self.log.info(f"Database '{database_name}' exists in the catalog")
        except glue.exceptions.EntityNotFoundException:
            self.log.info(f"Database '{database_name}' not found in catalog. Creating it...")
            glue.create_database(DatabaseInput={
                'Name': database_name,
                'Description': 'Created by Aurora-Athena ETL pipeline',
            })
            self.log.info(f"Created database '{database_name}' in the catalog")
            
            # Need to wait for the crawler to run again to populate tables
            self.log.info("Starting crawler again to populate the new database...")
            if glue.get_crawler(Name=crawl_n)["Crawler"]["State"] != "RUNNING":
                glue.start_crawler(Name=crawl_n)
            self._poll(lambda: glue.get_crawler(Name=crawl_n)["Crawler"], "State", "READY", ["FAILED"], f"Glue Crawler '{crawl_n}' (database population)")
            self.log.info("Waiting 30 seconds for catalog to update after crawler run...")
            time.sleep(30)
        
        # Now check for tables
        try:
            tables = glue.get_tables(DatabaseName=database_name)["TableList"]
            self.log.info(f"Found {len(tables)} tables in database '{database_name}'")
            
            if tables:
                # Use the first table if available and update the table name
                first_table = tables[0]["Name"]
                self.log.info(f"Found {len(tables)} tables in database '{database_name}'")
                if len(tables) == 0:
                    self.log.warning(f"No tables found in database '{database_name}'")
                    self.log.warning("Will use direct JDBC connections to employee tables in the ETL script")
                    source_tables = []  # We'll use direct JDBC connections instead
                else:
                    # Use the discovered tables
                    source_tables = tables
            else:
                self.log.warning(f"No tables found in database '{database_name}'")
                self.log.warning("Will use direct JDBC connections to employee tables in the ETL script")
                source_tables = []  # We'll use direct JDBC connections instead
        except glue.exceptions.EntityNotFoundException as e:
            self.log.error(f"Error accessing tables in database '{database_name}': {e}")
            # Continue with the original table name, but the job might fail
            self.log.warning(f"Will attempt to use original table name '{table_name}' but job may fail")
            
        # ---------- ETL script & job -----------------------------------------
        script_uri = self._create_and_upload_etl_script()
        job_p = prm["glue_etl_job"]
        job_n = job_p["job_name"]
        # Get JDBC connection details
        jdbc_url = prm["glue_connection"]["connection_properties"]["JDBC_CONNECTION_URL"]
        if "{ENDPOINT}" in jdbc_url:
            endpoint = self.clients["rds"].describe_db_clusters(
                DBClusterIdentifier=prm["aurora"]["db_cluster_identifier"]
            )["DBClusters"][0]["Endpoint"]
            jdbc_url = jdbc_url.replace("{ENDPOINT}", endpoint)
        
        jdbc_user = prm["aurora"]["master_username"]
        jdbc_password = self.cfg["aurora_pw"]
        
        # Prepare job arguments
        job_args = {
            "--job-bookmark-option": "job-bookmark-enable",
            "--enable-metrics": "true",
            "--SOURCE_DATABASE": prm["aurora"]["db_name"],  # Use the actual Aurora database name
            "--SOURCE_TABLE": "customers",  # Use the exact table name we created
            "--TARGET_S3_LOCATION": prm["glue_etl_job"]["default_arguments"]["--TARGET_S3_LOCATION"],
            "--JDBC_URL": jdbc_url,
            "--JDBC_USER": jdbc_user,
            "--JDBC_PASSWORD": jdbc_password,
            "--TempDir": self._get_s3_path(prm, "temp"),  # Add temp directory
            "--enable-continuous-cloudwatch-log": "true",  # Enable detailed logging
            "--enable-spark-ui": "true",  # Enable Spark UI for debugging
            "--spark-event-logs-path": self._get_s3_path(prm, "sparkHistoryLogs"),  # Spark history logs
            "--conf": "spark.sql.broadcastTimeout=1200"  # Increase broadcast timeout
        }
        
        try:
            glue.get_job(JobName=job_n)
            # Update the job with the correct parameters
            glue.update_job(
                JobName=job_n,
                JobUpdate={
                    "Role": role_arn,
                    "Command": {"Name": "glueetl", "ScriptLocation": script_uri, "PythonVersion": "3"},
                    "DefaultArguments": job_args,
                    "GlueVersion": "3.0",
                    "WorkerType": job_p.get("worker_type", "G.1X"),
                    "NumberOfWorkers": job_p.get("number_of_workers", 2),
                    "Timeout": job_p.get("timeout", 30),
                }
            )
            self.log.info(f"Updated Glue job {job_n} with JDBC connection parameters")
        except glue.exceptions.EntityNotFoundException:
            self.log.info("Creating Glue job %s", job_n)
            glue.create_job(
                Name=job_n,
                Role=role_arn,
                Command={"Name": "glueetl", "ScriptLocation": script_uri, "PythonVersion": "3"},
                DefaultArguments=job_args,
                GlueVersion="3.0",
                WorkerType=job_p.get("worker_type", "G.1X"),
                NumberOfWorkers=job_p.get("number_of_workers", 2),
                Timeout=job_p.get("timeout", 30),
            )
        run_id = glue.start_job_run(JobName=job_n)["JobRunId"]
        try:
            self._poll(lambda: glue.get_job_run(JobName=job_n, RunId=run_id)["JobRun"], "JobRunState", "SUCCEEDED", ["FAILED", "TIMEOUT", "STOPPED"], f"Glue ETL Job '{job_n}'")
        except RuntimeError as err:
            errmsg = glue.get_job_run(JobName=job_n, RunId=run_id)["JobRun"].get("ErrorMessage", "<no Glue error message>")
            self.log.error("Glue job failed: %s", errmsg)
            raise err

    # ─────────────────────────────── athena ──────────────────────────────────
    def setup_athena(self):
        ath = self.clients["athena"]
        prm = self.cfg["params"]
        ath_p = prm["athena"]
        out = ath_p["output_location"]
        db  = ath_p["database_name"]

        def q(query: str):
            qid = ath.start_query_execution(QueryString=query, ResultConfiguration={"OutputLocation": out})["QueryExecutionId"]
            self._poll(lambda: ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"], "Status.State", "SUCCEEDED", ["FAILED", "CANCELLED"], f"Athena Query {qid}")

        q(f"CREATE DATABASE IF NOT EXISTS {db}")
        # Use the table name from the Glue ETL job default arguments
        table = prm["glue_etl_job"]["default_arguments"]["--SOURCE_TABLE"].replace("aurora_", "")  # Remove 'aurora_' prefix
        tables = [table]  # Using a list for compatibility with the loop
        for tbl in tables:
            # Extract bucket and path from TARGET_S3_LOCATION
            target_s3_location = prm["glue_etl_job"]["default_arguments"]["--TARGET_S3_LOCATION"]
            # Remove trailing slash if present
            if target_s3_location.endswith('/'):
                target_s3_location = target_s3_location[:-1]
            loc = target_s3_location
            if tbl == "customers":
                ddl = f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS {db}.customers (
                      customer_id INT, 
                      name STRING, 
                      email STRING, 
                      registration_date DATE)
                    PARTITIONED BY (processing_time STRING)
                    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                    LOCATION '{loc}'
                    TBLPROPERTIES ('parquet.compression'='SNAPPY')"""
            elif tbl == "orders":
                ddl = f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS {db}.orders (
                      order_id INT, 
                      customer_id INT, 
                      order_date DATE, 
                      total_amount DECIMAL(10,2))
                    PARTITIONED BY (processing_time STRING)
                    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                    LOCATION '{loc}'
                    TBLPROPERTIES ('parquet.compression'='SNAPPY')"""
            else:
                continue
            q(ddl)
            q(f"MSCK REPAIR TABLE {db}.{tbl}")
        self.log.info("Athena ready.")

    # ───────────────────────── helper: ETL script ───────────────────────────
    def _create_and_upload_etl_script(self) -> str:
        prm = self.cfg["params"]
        script_dir = self.project_dir / "bin" / "scripts"
        script_dir.mkdir(parents=True, exist_ok=True)
        script_path = script_dir / "aurora_to_s3_etl.py"
        script_path.write_text(self._etl_script_content(), encoding="utf-8")

        # S3 URI from params
        s3_uri = prm["glue_etl_job"]["script_location"]  # e.g. s3://bucket/key
        bucket, key = s3_uri.replace("s3://", "").split("/", 1)
        self.clients["s3"].upload_file(str(script_path), bucket, key)
        self.log.info("Uploaded ETL script → %s", s3_uri)
        return s3_uri

    def _etl_script_content(self) -> str:
        """PySpark ETL that uses JDBC connection directly instead of Glue catalog."""
        prm = self.cfg["params"]
        aurora_params = prm["aurora"]
        conn_props = prm["glue_connection"]["connection_properties"]
        
        # Extract host from JDBC URL
        jdbc_url = conn_props["JDBC_CONNECTION_URL"]
        if "{ENDPOINT}" in jdbc_url:
            # Get the actual endpoint
            endpoint = self.clients["rds"].describe_db_clusters(
                DBClusterIdentifier=aurora_params["db_cluster_identifier"]
            )["DBClusters"][0]["Endpoint"]
            jdbc_url = jdbc_url.replace("{ENDPOINT}", endpoint)
        
        # Ensure database name is in JDBC URL
        if "databaseName=" not in jdbc_url:
            if "?" in jdbc_url:
                jdbc_url += f"&databaseName={aurora_params['db_name']}"
            else:
                jdbc_url += f"?databaseName={aurora_params['db_name']}"
        
        return r"""import sys
import time
import pymysql
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
from urllib.parse import urlparse, parse_qs

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_DATABASE', 'SOURCE_TABLE', 'TARGET_S3_LOCATION', 'JDBC_URL', 'JDBC_USER', 'JDBC_PASSWORD'])
sc = SparkContext()
ctx = GlueContext(sc)
job = Job(ctx)
job.init(args['JOB_NAME'], args)

# Process the specified table using JDBC connection directly
table = args['SOURCE_TABLE']
if table.startswith('aurora_'):
    # Remove the aurora_ prefix if present
    jdbc_table = table.replace('aurora_', '')
else:
    jdbc_table = table

print(f"Reading from JDBC: {args['JDBC_URL']} table: {jdbc_table}")

# First verify the table exists using pymysql
def verify_table_exists():
    # Parse JDBC URL to get host and database
    parsed_url = urlparse(args['JDBC_URL'].replace('jdbc:mysql://', ''))
    host = parsed_url.netloc.split(':')[0]
    port = 3306  # Default MySQL port
    if ':' in parsed_url.netloc:
        port = int(parsed_url.netloc.split(':')[1])
    
    # Get database name from URL parameters or path
    database = args['SOURCE_DATABASE']
    if 'databaseName' in args['JDBC_URL']:
        query_params = parse_qs(parsed_url.query)
        if 'databaseName' in query_params:
            database = query_params['databaseName'][0]
    
    print(f"Verifying table {jdbc_table} exists in database {database} on host {host}:{port}")
    
    # Try to connect and verify table exists
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = pymysql.connect(
                host=host,
                port=port,
                user=args['JDBC_USER'],
                password=args['JDBC_PASSWORD'],
                database=database
            )
            
            with conn.cursor() as cursor:
                # Check if table exists
                cursor.execute(f"SHOW TABLES LIKE '{jdbc_table}'")
                table_exists = cursor.fetchone() is not None
                
                if not table_exists:
                    print(f"Table '{jdbc_table}' does not exist in database '{database}'")
                    # Try with different case
                    cursor.execute("SHOW TABLES")
                    tables = [t[0] for t in cursor.fetchall()]
                    print(f"Available tables: {tables}")
                    
                    # Check for case-insensitive match
                    for db_table in tables:
                        if db_table.lower() == jdbc_table.lower():
                            print(f"Found table with different case: '{db_table}', will use this instead")
                            return db_table
                    
                    # If we get here, no matching table was found
                    if attempt < max_retries - 1:
                        print(f"Retrying in {2 ** attempt} seconds...")
                        time.sleep(2 ** attempt)
                    else:
                        raise Exception(f"Table '{jdbc_table}' not found in database '{database}'")
                else:
                    print(f"Table '{jdbc_table}' exists in database '{database}'")
                    return jdbc_table
            
        except Exception as e:
            print(f"Error verifying table: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {2 ** attempt} seconds...")
                time.sleep(2 ** attempt)
            else:
                raise
    
    return jdbc_table

# Verify table exists and get correct table name
try:
    verified_table = verify_table_exists()
    print(f"Using verified table name: {verified_table}")
    jdbc_table = verified_table
except Exception as e:
    print(f"Warning: Table verification failed: {str(e)}")
    print("Proceeding with original table name, but this may fail")

# Connect directly to the database using JDBC
conn_options = {
    "url": args['JDBC_URL'],
    "dbtable": jdbc_table,
    "user": args['JDBC_USER'],
    "password": args['JDBC_PASSWORD'],
    "fetchsize": "1000",
    "sessionInitStatement": "SET @@session.time_zone='+00:00';"
}

try:
    # Read directly from JDBC with retry logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt+1}/{max_retries} to read from JDBC")
            # First try to get schema to validate connection
            print("Testing JDBC connection by fetching schema...")
            schema_df = ctx.spark_session.read.format("jdbc") \
                .option("url", conn_options["url"]) \
                .option("dbtable", f"(SELECT * FROM {conn_options['dbtable']} LIMIT 1) as t") \
                .option("user", conn_options["user"]) \
                .option("password", conn_options["password"]) \
                .load()
            print(f"Schema test successful. Table schema: {schema_df.schema}")
            
            # Now read the actual data
            print(f"Reading data from table {conn_options['dbtable']}...")
            df = ctx.spark_session.read.format("jdbc").options(**conn_options).load()
            
            # Check if we got data
            row_count = df.count()
            print(f"Successfully read {row_count} rows from {jdbc_table}")
            
            if row_count > 0:
                # Add processing timestamp and metadata
                df = df.withColumn('processing_time', F.current_timestamp()) \
                      .withColumn('source_database', F.lit(args['SOURCE_DATABASE'])) \
                      .withColumn('source_table', F.lit(jdbc_table))
                
                # Convert to DynamicFrame for writing
                print("Converting to DynamicFrame...")
                out = DynamicFrame.fromDF(df, ctx, f'{table}_out')
                
                # Write to S3
                target = args['TARGET_S3_LOCATION']
                print(f"Writing {row_count} rows to {target}...")
                ctx.write_dynamic_frame.from_options(
                    out, 
                    connection_type='s3', 
                    connection_options={
                        'path': target, 
                        'partitionKeys': ['processing_time', 'source_database']
                    }, 
                    format='parquet'
                )
                print(f'Successfully wrote {row_count} rows from {jdbc_table} → {target}')
                break  # Success, exit retry loop
            else:
                print(f'Table {jdbc_table} has no records. Creating empty file as marker.')
                # Write an empty dataframe with the schema as a marker
                empty_df = ctx.spark_session.createDataFrame([], schema_df.schema) \
                           .withColumn('processing_time', F.current_timestamp()) \
                           .withColumn('source_database', F.lit(args['SOURCE_DATABASE'])) \
                           .withColumn('source_table', F.lit(jdbc_table)) \
                           .withColumn('is_empty_marker', F.lit(True))
                
                empty_out = DynamicFrame.fromDF(empty_df, ctx, f'{table}_empty')
                target = args['TARGET_S3_LOCATION'] + "/empty_markers/"
                ctx.write_dynamic_frame.from_options(
                    empty_out, 
                    connection_type='s3', 
                    connection_options={'path': target}, 
                    format='parquet'
                )
                print(f'Wrote empty marker for {jdbc_table} → {target}')
                break  # Success with empty table, exit retry loop
        except Exception as e:
            print(f"Error on attempt {attempt+1}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"All {max_retries} attempts failed. Giving up.")
                raise
except Exception as e:
    print(f"Error processing table {jdbc_table}: {str(e)}")
    # Print detailed error information
    import traceback
    traceback.print_exc()
    raise

print("Job completed successfully")
job.commit()
"""



    # ─────────────────────── network and security ──────────────────────────
    def _ensure_security_group_access(self, security_group_id):
        """Ensure that the security group allows inbound access from Glue."""
        self.log.info(f"Checking security group {security_group_id} inbound rules...")
        ec2 = self.clients["ec2"]
        
        try:
            response = ec2.describe_security_groups(GroupIds=[security_group_id])
            sg_rules = response["SecurityGroups"][0]["IpPermissions"]
            
            # Check if MySQL port is already open
            mysql_rule_exists = False
            for rule in sg_rules:
                if rule.get("FromPort") == 3306 and rule.get("ToPort") == 3306:
                    # Check if it allows connections from anywhere
                    for ip_range in rule.get("IpRanges", []):
                        if ip_range.get("CidrIp") == "0.0.0.0/0":
                            mysql_rule_exists = True
                            break
                    if mysql_rule_exists:
                        break
            
            if not mysql_rule_exists:
                self.log.info(f"Adding MySQL access rule to security group {security_group_id}")
                # Remove any existing MySQL rules first
                for rule in sg_rules:
                    if rule.get("FromPort") == 3306 and rule.get("ToPort") == 3306:
                        try:
                            ec2.revoke_security_group_ingress(
                                GroupId=security_group_id,
                                IpPermissions=[rule]
                            )
                            self.log.info(f"Removed existing MySQL rule from security group {security_group_id}")
                        except Exception as e:
                            self.log.warning(f"Could not revoke existing rule: {e}")
                
                # Add new rule allowing connections from anywhere
                ec2.authorize_security_group_ingress(
                    GroupId=security_group_id,
                    IpPermissions=[
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 3306,
                            "ToPort": 3306,
                            "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Allow MySQL from anywhere"}]
                        }
                    ]
                )
                self.log.info(f"Added MySQL access rule to security group {security_group_id}")
            else:
                self.log.info(f"Security group {security_group_id} already has MySQL access rule")
                
        except Exception as e:
            self.log.error(f"Error configuring security group: {e}")
            raise

            self.log.warning(f"Could not verify or update security group rules: {e}")
            
    def _setup_vpc_endpoints(self, vpc_id, subnet_id, security_group_id):
        """Create necessary VPC endpoints for Glue to communicate with services within the VPC."""
        ec2 = self.clients["ec2"]
        self.log.info("Setting up VPC endpoints for AWS Glue connectivity...")
        
        # Check if VPC endpoints already exist
        try:
            endpoints = ec2.describe_vpc_endpoints(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
            existing_endpoints = {endpoint.get("ServiceName"): endpoint.get("VpcEndpointId") 
                                for endpoint in endpoints.get("VpcEndpoints", [])}
            
            # Define required endpoints
            required_endpoints = {
                f"com.amazonaws.{self.cfg['region']}.glue": "Interface",
                f"com.amazonaws.{self.cfg['region']}.s3": "Gateway"
            }
            
            for service_name, endpoint_type in required_endpoints.items():
                if service_name not in existing_endpoints:
                    self.log.info(f"Creating VPC endpoint for {service_name}")
                    
                    if endpoint_type == "Interface":
                        ec2.create_vpc_endpoint(
                            VpcEndpointType=endpoint_type,
                            VpcId=vpc_id,
                            ServiceName=service_name,
                            SubnetIds=[subnet_id],
                            SecurityGroupIds=[security_group_id],
                            PrivateDnsEnabled=True
                        )
                    else:  # Gateway endpoint
                        ec2.create_vpc_endpoint(
                            VpcEndpointType=endpoint_type,
                            VpcId=vpc_id,
                            ServiceName=service_name,
                            RouteTableIds=self._get_route_table_ids(vpc_id, subnet_id)
                        )
                    
                    self.log.info(f"Created {endpoint_type} VPC endpoint for {service_name}")
                else:
                    self.log.info(f"VPC endpoint for {service_name} already exists")
                    
        except Exception as e:
            self.log.warning(f"Error setting up VPC endpoints: {e}")
            
    def _get_route_table_ids(self, vpc_id, subnet_id):
        """Get route table IDs associated with a subnet."""
        ec2 = self.clients["ec2"]
        
        try:
            # First try to get route tables associated with the subnet
            response = ec2.describe_route_tables(Filters=[
                {"Name": "association.subnet-id", "Values": [subnet_id]}
            ])
            
            route_table_ids = [rt.get("RouteTableId") for rt in response.get("RouteTables", [])]
            
            # If no route tables are associated with the subnet, get the main route table for the VPC
            if not route_table_ids:
                response = ec2.describe_route_tables(Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "association.main", "Values": ["true"]}
                ])
                route_table_ids = [rt.get("RouteTableId") for rt in response.get("RouteTables", [])]
                
            return route_table_ids
        except Exception as e:
            self.log.warning(f"Error getting route table IDs: {e}")
            return []
            
    def _initialize_database_tables(self, endpoint):
        """Initialize required tables in the Aurora database."""
        self.log.info("Initializing database tables...")
        
        # Wait for Aurora instance to be fully available
        self.log.info("Waiting for Aurora instance to be fully available...")
        time.sleep(10)  # Give Aurora some time to stabilize
        
        # Use a retry mechanism for database connection
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                # Use pymysql to connect to the database and create tables
                self.log.info(f"Connecting to database at {endpoint} (attempt {attempt+1}/{max_retries})")
                conn = pymysql.connect(
                    host=endpoint,
                    user=self.cfg["params"]["aurora"]["master_username"],
                    password=self.cfg["aurora_pw"],
                    database=self.cfg["params"]["aurora"]["db_name"],
                    connect_timeout=30  # Increase connection timeout
                )
                
                with conn.cursor() as cursor:
                    # First check if the table exists to avoid case sensitivity issues
                    cursor.execute("SHOW TABLES LIKE 'customers'")
                    table_exists = cursor.fetchone() is not None
                    
                    if not table_exists:
                        self.log.info("Creating customers table...")
                        # Create customers table with exact case matching what Glue will look for
                        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS customers (
                            customer_id INT PRIMARY KEY,
                            name VARCHAR(100) NOT NULL,
                            email VARCHAR(100),
                            registration_date DATE
                        )
                        """)
                        
                    # Check if the table is empty and insert sample data if needed
                    cursor.execute("SELECT COUNT(*) FROM customers")
                    count = cursor.fetchone()[0]
                    
                    if count == 0:
                        self.log.info("Inserting sample data into customers table")
                        # Insert sample data
                        sample_data = [
                            (1, 'John Doe', 'john@example.com', '2023-01-15'),
                            (2, 'Jane Smith', 'jane@example.com', '2023-02-20'),
                            (3, 'Bob Johnson', 'bob@example.com', '2023-03-10'),
                            (4, 'Alice Brown', 'alice@example.com', '2023-04-05'),
                            (5, 'Charlie Davis', 'charlie@example.com', '2023-05-12')
                        ]
                        
                        cursor.executemany("""
                        INSERT INTO customers (customer_id, name, email, registration_date)
                        VALUES (%s, %s, %s, %s)
                        """, sample_data)
                        
                        conn.commit()
                        self.log.info(f"Inserted {len(sample_data)} sample records into customers table")
                    else:
                        self.log.info(f"Customers table already contains {count} records")
                
                # Verify the table is accessible with a simple query
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM customers LIMIT 1")
                    result = cursor.fetchone()
                    self.log.info(f"Verified table access: {result is not None}")
                    
                conn.close()
                self.log.info("Database tables initialized successfully")
                return  # Success, exit the retry loop
                
            except Exception as e:
                self.log.warning(f"Error initializing database tables (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    self.log.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    self.log.error("Failed to initialize database tables after multiple attempts")
                    # Continue execution even if table initialization fails
    
    # ─────────────────────────────── run ────────────────────────────────────
    # Set up IAM role for Glue
    def setup_iam_role(self):
        """Set up or retrieve the IAM role for AWS Glue"""
        try:
            # Check if role ARN is provided in config
            if "role_arn" in self.cfg.get("params", {}).get("glue_job", {}):
                role_arn = self.cfg["params"]["glue_job"]["role_arn"]
                self.log.info(f"Using existing IAM role from config: {role_arn}")
                return role_arn
                
            # Check if role name is provided in config
            role_name = self.cfg.get("params", {}).get("glue_job", {}).get("role_name", "AuroraAthenaETLRole")
                
            # Try to get existing role
            try:
                response = self.clients["iam"].get_role(RoleName=role_name)
                role_arn = response["Role"]["Arn"]
                self.log.info(f"Found existing IAM role: {role_arn}")
                return role_arn
            except self.clients["iam"].exceptions.NoSuchEntityException:
                self.log.info(f"IAM role {role_name} not found, creating it...")
                    
            # Create a new role with necessary policies
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "glue.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }]
            }
                
            response = self.clients["iam"].create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="Role for Aurora-Athena ETL Pipeline Glue jobs"
            )
                
            role_arn = response["Role"]["Arn"]
                
            # Attach necessary policies
            policy_arns = [
                "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
                "arn:aws:iam::aws:policy/AmazonS3FullAccess",  # For S3 access
                "arn:aws:iam::aws:policy/AmazonRDSFullAccess"   # For RDS access
            ]
                
            for policy_arn in policy_arns:
                self.clients["iam"].attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
                self.log.info(f"Attached policy {policy_arn} to role {role_name}")
                
            # Wait for role to propagate
            time.sleep(10)
                
            self.log.info(f"Created IAM role: {role_arn}")
            return role_arn
                
        except Exception as e:
            self.log.error(f"Error creating IAM role: {e}")
            # Use a default role name pattern instead of calling STS
            account_id = "YOUR_ACCOUNT_ID"  # This is a placeholder
            default_role = f"arn:aws:iam::{account_id}:role/service-role/AWSGlueServiceRole"
            self.log.warning(f"Falling back to default role pattern: {default_role}")
            return default_role
    
    def run(self):
        try:
            self.log.info("Starting Aurora-Athena ETL Pipeline")
            
            # Step 1: Set up Aurora and make it publicly accessible
            self.log.info("Step 1: Setting up Aurora database...")
            aurora_endpoint = self.setup_aurora()
            self.log.info(f"Aurora endpoint: {aurora_endpoint}")
            
            # Step 2: Get security group and subnet information
            self.log.info("Step 2: Getting network configuration...")
            # Get security group ID from Aurora configuration
            security_group_id = self.cfg["params"]["aurora"]["vpc_security_group_ids"][0]
            # Get subnet ID from Glue connection physical requirements
            subnet_id = self.cfg["params"]["glue_connection"]["physical_connection_requirements"]["subnet_id"]
            
            # Get VPC ID from subnet
            vpc_response = self.clients["ec2"].describe_subnets(SubnetIds=[subnet_id])
            vpc_id = vpc_response["Subnets"][0]["VpcId"]
            self.log.info(f"VPC ID: {vpc_id}, Subnet ID: {subnet_id}, Security Group ID: {security_group_id}")
            
            # Step 3: Ensure security group allows inbound MySQL traffic
            self.log.info("Step 3: Configuring security group access...")
            self._ensure_security_group_access(security_group_id)
            
            # Step 4: Set up VPC endpoints for Glue and S3
            self.log.info("Step 4: Setting up VPC endpoints...")
            self._setup_vpc_endpoints(vpc_id, subnet_id, security_group_id)
            
            # Step 5: Initialize database tables with sample data
            self.log.info("Step 5: Initializing database tables...")
            self._initialize_database_tables(aurora_endpoint)
            
            # Step 6: Set up IAM role for Glue
            self.log.info("Step 6: Setting up IAM role...")
            role_arn = self.setup_iam_role()
            self.log.info(f"IAM Role ARN: {role_arn}")
            
            # Step 7: Set up Glue connection, crawler, and job
            self.log.info("Step 7: Setting up AWS Glue...")
            self.setup_glue(role_arn, aurora_endpoint)
            
            # Step 8: Wait for a moment to ensure all resources are properly set up
            self.log.info("Step 8: Waiting for resources to stabilize...")
            time.sleep(10)
            
            # Step 9: Set up Athena
            self.log.info("Step 9: Setting up Athena...")
            
        except Exception as e:
            self.log.error(f"Error setting up IAM role: {e}")
            # Fallback to a default role if available in AWS account
            default_role = "arn:aws:iam::" + self.clients["sts"].get_caller_identity()["Account"] + ":role/service-role/AWSGlueServiceRole"
            self.log.warning(f"Falling back to default role: {default_role}")
            return default_role
        """Verify end-to-end connectivity between all components."""
        try:
            self.log.info("Verifying Aurora database connectivity...")
            # Test direct connection to Aurora
            import pymysql
            conn = pymysql.connect(
                host=aurora_endpoint,
                user=self.cfg["params"]["aurora"]["master_username"],
                password=self.cfg["aurora_pw"],
                database=self.cfg["params"]["aurora"]["db_name"]
            )
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = [t[0] for t in cursor.fetchall()]
                self.log.info(f"Successfully connected to Aurora. Available tables: {tables}")
                
                # Verify customers table exists
                cursor.execute("SHOW TABLES LIKE 'customers'")
                if cursor.fetchone():
                    cursor.execute("SELECT COUNT(*) FROM customers")
                    count = cursor.fetchone()[0]
                    self.log.info(f"Customers table exists with {count} records")
                else:
                    self.log.warning("Customers table not found in Aurora database")
            conn.close()
            
            # Verify Glue connection
            self.log.info("Verifying Glue connection...")
            glue = self.clients["glue"]
            conn_name = self.cfg["params"]["glue_connection"]["connection_name"]
            try:
                glue.get_connection(Name=conn_name)
                self.log.info(f"Glue connection {conn_name} exists")
            except Exception as e:
                self.log.warning(f"Error verifying Glue connection: {e}")
                
            # Verify S3 bucket
            self.log.info("Verifying S3 bucket...")
            s3 = self.clients["s3"]
            # Extract bucket name from an existing S3 path
            prm = self.cfg["params"]
            try:
                # Get a bucket name from any available S3 path
                s3_path = self._get_s3_path(prm, "").rstrip("/")
                bucket = s3_path.replace("s3://", "").split("/")[0]
                
                s3.head_bucket(Bucket=bucket)
                self.log.info(f"S3 bucket {bucket} exists and is accessible")
            except Exception as e:
                self.log.warning(f"Error verifying S3 bucket: {e}")
                
            self.log.info("End-to-end connectivity verification completed")
            return True
        except Exception as e:
            self.log.error(f"Error verifying end-to-end connectivity: {e}")
            return False

# ... (rest of the code remains the same)
#  entrypoint
# ════════════════════════════════════════════════════════════════════════════

def main() -> int:
    try:
        AuroraAthenaETLPipeline().run()
        return 0
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.exception("Pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
