#!/usr/bin/env python3

import boto3
import pymysql
import json
import os
import sys
import time
import logging
from datetime import datetime

class EmployeeTablesExporter:
    def __init__(self):
        # Set up logging
        logging.basicConfig(
            format='%(asctime)s | %(levelname)-8s | %(message)s',
            level=logging.INFO
        )
        self.log = logging.getLogger('EmployeeTablesExporter')
        
        # Get AWS account ID
        session = boto3.Session()
        sts_client = session.client('sts')
        self.account_id = sts_client.get_caller_identity()["Account"]
        self.region = session.region_name or 'us-east-1'
        
        # Set up AWS clients
        self.clients = {
            "s3": session.client('s3'),
            "glue": session.client('glue'),
            "athena": session.client('athena'),
            "iam": session.client('iam')
        }
        
        # Configuration
        self.config = {
            "aurora": {
                "endpoint": "aurora-mysql-cluster.cluster-c472k0sskdbg.us-east-1.rds.amazonaws.com",
                "port": 3306,
                "username": "admin",
                "password": "Admin123!",
                "database": "employees"
            },
            "s3": {
                "bucket": f"glue-target-bucket-{self.account_id}",
                "prefix": "data/"
            },
            "glue": {
                "database": "aurora_catalog",
                "role_name": "AuroraAthenaETLRole"
            }
        }
    
    def get_employee_tables(self):
        """Get list of all employee-related tables"""
        try:
            # Connect to Aurora MySQL
            conn = pymysql.connect(
                host=self.config["aurora"]["endpoint"],
                port=self.config["aurora"]["port"],
                user=self.config["aurora"]["username"],
                password=self.config["aurora"]["password"],
                database=self.config["aurora"]["database"]
            )
            
            # Query for all tables
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = [table[0] for table in cursor.fetchall()]
                
                # Filter for employee-related tables (exclude 'customers')
                employee_tables = [table for table in tables if table != 'customers']
                
                self.log.info(f"Found {len(employee_tables)} employee-related tables: {', '.join(employee_tables)}")
                return employee_tables
        except Exception as e:
            self.log.error(f"Error getting employee tables: {e}")
            return []
        finally:
            if 'conn' in locals() and conn:
                conn.close()
    
    def get_role_arn(self):
        """Get IAM role ARN for Glue jobs"""
        role_name = self.config["glue"]["role_name"]
        try:
            response = self.clients["iam"].get_role(RoleName=role_name)
            role_arn = response["Role"]["Arn"]
            self.log.info(f"Using existing IAM role: {role_arn}")
            return role_arn
        except Exception as e:
            self.log.error(f"Error getting IAM role {role_name}: {e}")
            # Fallback to default pattern
            default_role_arn = f"arn:aws:iam::{self.account_id}:role/{role_name}"
            self.log.warning(f"Using default role ARN pattern: {default_role_arn}")
            return default_role_arn
    
    def run_etl_job(self, table_name):
        """Run Glue ETL job for a specific table"""
        try:
            # ETL script location
            script_location = f"s3://aws-glue-scripts-{self.account_id}-{self.region}/admin/aurora-to-s3-etl.py"
            
            # S3 target location
            target_location = f"s3://{self.config['s3']['bucket']}/{self.config['s3']['prefix']}{table_name}/"
            
            # JDBC connection URL
            jdbc_url = f"jdbc:mysql://{self.config['aurora']['endpoint']}:{self.config['aurora']['port']}/{self.config['aurora']['database']}"
            
            # Job name
            job_name = f"aurora-to-s3-etl-{table_name}"
            
            # Check if job already exists
            try:
                self.clients["glue"].get_job(JobName=job_name)
                self.log.info(f"Job {job_name} already exists")
            except Exception:
                # Create new job
                self.log.info(f"Creating Glue job {job_name}")
                self.clients["glue"].create_job(
                    Name=job_name,
                    Role=self.get_role_arn(),
                    Command={
                        "Name": "glueetl",
                        "ScriptLocation": script_location,
                        "PythonVersion": "3"
                    },
                    DefaultArguments={
                        "--SOURCE_DATABASE": self.config["aurora"]["database"],
                        "--SOURCE_TABLE": table_name,
                        "--TARGET_S3_LOCATION": target_location,
                        "--JDBC_URL": jdbc_url,
                        "--JDBC_USER": self.config["aurora"]["username"],
                        "--JDBC_PASSWORD": self.config["aurora"]["password"],
                        "--job-language": "python"
                    },
                    GlueVersion="3.0"
                )
            
            # Start job run
            self.log.info(f"Starting Glue ETL job for table {table_name}")
            response = self.clients["glue"].start_job_run(
                JobName=job_name,
                Arguments={
                    "--SOURCE_DATABASE": self.config["aurora"]["database"],
                    "--SOURCE_TABLE": table_name,
                    "--TARGET_S3_LOCATION": target_location,
                    "--JDBC_URL": jdbc_url,
                    "--JDBC_USER": self.config["aurora"]["username"],
                    "--JDBC_PASSWORD": self.config["aurora"]["password"]
                }
            )
            
            job_run_id = response["JobRunId"]
            self.log.info(f"Job {job_name} started with run ID: {job_run_id}")
            
            # Monitor job
            status = "STARTING"
            while status in ["STARTING", "RUNNING", "STOPPING"]:
                time.sleep(30)  # Check every 30 seconds
                response = self.clients["glue"].get_job_run(JobName=job_name, RunId=job_run_id)
                status = response["JobRun"]["JobRunState"]
                self.log.info(f"Job {job_name} status: {status}")
            
            if status == "SUCCEEDED":
                self.log.info(f"ETL job for {table_name} completed successfully")
                return True, target_location
            else:
                self.log.error(f"ETL job for {table_name} failed with status: {status}")
                return False, target_location
        except Exception as e:
            self.log.error(f"Error running ETL job for {table_name}: {e}")
            return False, None
    
    def create_crawler(self, table_name, s3_location):
        """Create and run Glue crawler to catalog S3 data"""
        try:
            crawler_name = f"aurora-{table_name}-crawler"
            
            # Check if crawler already exists
            try:
                self.clients["glue"].get_crawler(Name=crawler_name)
                self.log.info(f"Crawler {crawler_name} already exists")
            except Exception:
                # Create new crawler
                self.log.info(f"Creating Glue crawler {crawler_name}")
                self.clients["glue"].create_crawler(
                    Name=crawler_name,
                    Role=self.get_role_arn(),
                    DatabaseName=self.config["glue"]["database"],
                    Targets={
                        "S3Targets": [
                            {"Path": s3_location}
                        ]
                    },
                    SchemaChangePolicy={
                        "UpdateBehavior": "UPDATE_IN_DATABASE",
                        "DeleteBehavior": "LOG"
                    }
                )
            
            # Start crawler
            self.log.info(f"Starting Glue crawler {crawler_name}")
            self.clients["glue"].start_crawler(Name=crawler_name)
            
            # Monitor crawler
            status = None
            while status != "READY":
                time.sleep(30)  # Check every 30 seconds
                response = self.clients["glue"].get_crawler(Name=crawler_name)
                status = response["Crawler"]["State"]
                self.log.info(f"Crawler {crawler_name} status: {status}")
                
                if status == "FAILED":
                    self.log.error(f"Crawler {crawler_name} failed")
                    return False
            
            # Wait for catalog to update
            self.log.info("Waiting for Glue catalog to update (30 seconds)...")
            time.sleep(30)
            return True
        except Exception as e:
            self.log.error(f"Error with crawler for {table_name}: {e}")
            return False
    
    def test_athena_query(self, table_name):
        """Test query against table in Athena"""
        try:
            # Output location for query results
            query_results_bucket = f"aws-athena-query-results-{self.account_id}-{self.region}"
            output_location = f"s3://{query_results_bucket}/"
            
            # Ensure results bucket exists
            try:
                self.clients["s3"].head_bucket(Bucket=query_results_bucket)
            except Exception:
                self.log.info(f"Creating Athena query results bucket: {query_results_bucket}")
                self.clients["s3"].create_bucket(Bucket=query_results_bucket)
            
            # Start query execution
            self.log.info(f"Testing Athena query on table {table_name}")
            query = f'SELECT * FROM "{self.config["glue"]["database"]}"."{table_name}" LIMIT 5'
            response = self.clients["athena"].start_query_execution(
                QueryString=query,
                ResultConfiguration={
                    "OutputLocation": output_location
                }
            )
            
            execution_id = response["QueryExecutionId"]
            
            # Wait for query completion
            status = "QUEUED"
            while status in ["QUEUED", "RUNNING"]:
                time.sleep(5)
                response = self.clients["athena"].get_query_execution(QueryExecutionId=execution_id)
                status = response["QueryExecution"]["Status"]["State"]
            
            if status == "SUCCEEDED":
                # Get query results
                results = self.clients["athena"].get_query_results(QueryExecutionId=execution_id)
                
                # Format and print results
                columns = [col["Name"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
                self.log.info(f"Query successful. Column names: {', '.join(columns)}")
                
                rows = results["ResultSet"]["Rows"]
                if len(rows) > 1:  # First row is column headers
                    self.log.info(f"Found {len(rows)-1} rows in table {table_name}")
                    return True
                else:
                    self.log.warning(f"Table {table_name} exists but contains no data")
                    return True
            else:
                self.log.error(f"Athena query failed with status: {status}")
                if "StateChangeReason" in response["QueryExecution"]["Status"]:
                    self.log.error(f"Reason: {response['QueryExecution']['Status']['StateChangeReason']}")
                return False
        except Exception as e:
            self.log.error(f"Error testing Athena query for {table_name}: {e}")
            return False
    
    def process_table(self, table_name):
        """Process a single table: export to S3, create crawler, test query"""
        self.log.info(f"\n===== Processing table: {table_name} =====")
        
        # Run ETL job to export table to S3
        etl_success, s3_location = self.run_etl_job(table_name)
        if not etl_success:
            self.log.error(f"Failed to export table {table_name} to S3")
            return False
        
        # Create and run crawler to catalog data
        crawler_success = self.create_crawler(table_name, s3_location)
        if not crawler_success:
            self.log.error(f"Failed to catalog table {table_name} in Glue")
            return False
        
        # Test query against table in Athena
        query_success = self.test_athena_query(table_name)
        if not query_success:
            self.log.error(f"Failed to query table {table_name} in Athena")
            return False
        
        self.log.info(f"Successfully processed table {table_name}")
        return True
    
    def run(self):
        """Main execution method"""
        self.log.info("Starting employee tables export to Athena")
        
        # Get list of employee tables
        employee_tables = self.get_employee_tables()
        if not employee_tables:
            self.log.error("No employee tables found")
            return False
        
        # Process each table
        results = []
        for table in employee_tables:
            result = self.process_table(table)
            results.append((table, result))
        
        # Summary
        self.log.info("\n===== Export Summary =====")
        successful = [table for table, result in results if result]
        failed = [table for table, result in results if not result]
        
        self.log.info(f"Successfully exported {len(successful)} tables: {', '.join(successful)}")
        if failed:
            self.log.error(f"Failed to export {len(failed)} tables: {', '.join(failed)}")
        
        return len(failed) == 0

# Main execution
if __name__ == "__main__":
    exporter = EmployeeTablesExporter()
    success = exporter.run()
    sys.exit(0 if success else 1)