#!/usr/bin/env python3
"""
Aurora AWSleSS - AWS Resource Cleanup Script
======================================
This script removes all AWS resources created by the Aurora-Athena ETL Pipeline,
giving you a clean slate for future runs.
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("aurora_awsless.log"),
    ],
)
LOGGER = logging.getLogger("AuroraAWSless")


# ════════════════════════════════════════════════════════════════════════════
#  Cleanup implementation
# ════════════════════════════════════════════════════════════════════════════
class AuroraAWSlessCleanup:
    """AWS resource cleanup for Aurora-Athena ETL Pipeline."""

    RETRY_SLEEP = 30  # seconds between state polls

    # ─────────────────────────────── init ───────────────────────────────────
    def __init__(self) -> None:
        self.project_dir = Path(__file__).resolve().parents[2]
        self.log = LOGGER  # shorthand
        self.cfg = self._load_config()  # validated config dict
        self.clients = self.cfg["clients"]

    def _load_config(self) -> Dict[str, Any]:
        self.log.info("Loading configuration …")

        # .env --------------------------------------------------------------------------------------------------
        env_path = self.project_dir / "bin" / ".env"
        if not env_path.exists():
            raise FileNotFoundError(f"Missing .env file: {env_path}")
        load_dotenv(env_path)

        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        profile = os.getenv("AWS_PROFILE")

        # parameters.json ---------------------------------------------------------------------------------------
        params_path = self.project_dir / "bin" / "parameters.json"
        if not params_path.exists():
            raise FileNotFoundError(f"Missing parameters file: {params_path}")
        with open(params_path, "r", encoding="utf-8") as f:
            params = json.load(f)

        # AWS session ------------------------------------------------------------------------------------------
        if access_key and secret_key:
            self.log.info("Creating boto3 session using access keys")
            session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
        elif profile:
            self.log.info("Creating boto3 session using profile %s", profile)
            session = boto3.Session(profile_name=profile, region_name=region)
        else:
            raise RuntimeError("No AWS credentials found in .env")

        mk = session.client
        return {
            "region": region,
            "params": params,
            "session": session,
            "clients": {svc: mk(svc) for svc in ("rds", "ec2", "iam", "glue", "s3", "athena", "logs")},
        }

    def _poll(self, fn, key: str, ok: str, bad: List[str]):
        """Poll callable ``fn`` until ``fn()[key]`` is *ok* or *bad*.
        
        If key contains dots, it will be treated as a nested key path.
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
                
            self.log.info("Status = %s", status)
            if status == ok:
                return
            if status in bad:
                raise RuntimeError(f"Process ended with state {status}")
            time.sleep(self.RETRY_SLEEP)

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
        
        # If not found, try Athena output location
        elif "athena" in prm and "output_location" in prm["athena"]:
            s3_path = prm["athena"]["output_location"]
            
        if not s3_path:
            raise ValueError("Could not find any S3 path in configuration to extract bucket name")
        
        # Extract bucket name from s3://bucket-name/path/to/something
        parts = s3_path.replace("s3://", "").split("/")
        bucket = parts[0]
        
        return f"s3://{bucket}/{folder_name}/"

    def run(self):
        """Run the cleanup process."""
        try:
            self.log.info("Starting Aurora AWSleSS Cleanup")
            
            # Step 1: Clean up Athena resources
            self.log.info("Step 1: Cleaning up Athena resources...")
            self._cleanup_athena()
            
            # Step 2: Clean up Glue resources
            self.log.info("Step 2: Cleaning up Glue resources...")
            self._cleanup_glue()
            
            # Step 3: Clean up S3 resources
            self.log.info("Step 3: Cleaning up S3 resources...")
            self._cleanup_s3()
            
            # Step 4: Clean up Aurora resources
            self.log.info("Step 4: Cleaning up Aurora resources...")
            self._cleanup_aurora()
            
            # Step 5: Clean up IAM resources
            self.log.info("Step 5: Cleaning up IAM resources...")
            self._cleanup_iam()
            
            # Step 6: Clean up VPC endpoints
            self.log.info("Step 6: Cleaning up VPC endpoints...")
            self._cleanup_vpc_endpoints()
            
            self.log.info("Aurora AWSleSS Cleanup completed successfully")
            return True
        except Exception as e:
            self.log.error(f"Error in cleanup process: {str(e)}")
            import traceback
            self.log.error(traceback.format_exc())
            return False
    
    def _cleanup_athena(self):
        """Clean up Athena resources."""
        athena = self.clients["athena"]
        prm = self.cfg["params"]
        db_name = prm["athena"]["database_name"]
        
        try:
            # Drop all tables in the database
            self.log.info(f"Dropping all tables in Athena database '{db_name}'...")
            output_location = prm["athena"]["output_location"]
            
            # First, list all tables in the database
            glue = self.clients["glue"]
            try:
                tables = glue.get_tables(DatabaseName=db_name)["TableList"]
                for table in tables:
                    table_name = table["Name"]
                    self.log.info(f"Dropping table '{table_name}' from database '{db_name}'...")
                    query = f"DROP TABLE IF EXISTS {db_name}.{table_name}"
                    
                    try:
                        execution = athena.start_query_execution(
                            QueryString=query,
                            ResultConfiguration={"OutputLocation": output_location}
                        )
                        self._poll(
                            lambda: athena.get_query_execution(QueryExecutionId=execution["QueryExecutionId"])["QueryExecution"],
                            "Status.State",
                            "SUCCEEDED",
                            ["FAILED", "CANCELLED"]
                        )
                    except Exception as e:
                        self.log.warning(f"Error dropping table '{table_name}': {e}")
            except Exception as e:
                self.log.warning(f"Error listing tables in database '{db_name}': {e}")
            
            # Drop the database itself
            self.log.info(f"Dropping Athena database '{db_name}'...")
            try:
                query = f"DROP DATABASE IF EXISTS {db_name} CASCADE"
                execution = athena.start_query_execution(
                    QueryString=query,
                    ResultConfiguration={"OutputLocation": output_location}
                )
                self._poll(
                    lambda: athena.get_query_execution(QueryExecutionId=execution["QueryExecutionId"])["QueryExecution"],
                    "Status.State",
                    "SUCCEEDED",
                    ["FAILED", "CANCELLED"]
                )
            except Exception as e:
                self.log.warning(f"Error dropping database '{db_name}': {e}")
                
            self.log.info("Athena resources cleanup completed")
        except Exception as e:
            self.log.warning(f"Error during Athena cleanup: {e}")
    
    def _cleanup_glue(self):
        """Clean up Glue resources."""
        glue = self.clients["glue"]
        prm = self.cfg["params"]
        
        # Clean up Glue job
        job_name = prm["glue_etl_job"]["job_name"]
        try:
            self.log.info(f"Deleting Glue job '{job_name}'...")
            glue.delete_job(JobName=job_name)
            self.log.info(f"Glue job '{job_name}' deleted successfully")
        except Exception as e:
            self.log.warning(f"Error deleting Glue job '{job_name}': {e}")
        
        # Clean up Glue crawler
        crawler_name = prm["glue_crawler"]["crawler_name"]
        try:
            # Check if crawler is running and stop it if needed
            try:
                crawler_info = glue.get_crawler(Name=crawler_name)
                if crawler_info["Crawler"]["State"] == "RUNNING":
                    self.log.info(f"Stopping running crawler '{crawler_name}'...")
                    glue.stop_crawler(Name=crawler_name)
                    
                    # Wait for crawler to stop
                    max_wait_time = 300  # 5 minutes
                    wait_interval = 10  # 10 seconds
                    elapsed_time = 0
                    
                    while elapsed_time < max_wait_time:
                        crawler_info = glue.get_crawler(Name=crawler_name)
                        if crawler_info["Crawler"]["State"] != "RUNNING":
                            break
                        
                        time.sleep(wait_interval)
                        elapsed_time += wait_interval
                        self.log.info(f"Waiting for crawler '{crawler_name}' to stop...")
                    
                    if elapsed_time >= max_wait_time:
                        self.log.warning(f"Timed out waiting for crawler '{crawler_name}' to stop")
            except Exception as e:
                self.log.warning(f"Error checking crawler state: {e}")
            
            # Delete the crawler
            self.log.info(f"Deleting Glue crawler '{crawler_name}'...")
            glue.delete_crawler(Name=crawler_name)
            self.log.info(f"Glue crawler '{crawler_name}' deleted successfully")
        except Exception as e:
            self.log.warning(f"Error deleting Glue crawler '{crawler_name}': {e}")
        
        # Clean up Glue database
        db_name = prm["glue_crawler"]["database_name"]
        try:
            self.log.info(f"Deleting Glue database '{db_name}'...")
            glue.delete_database(Name=db_name)
            self.log.info(f"Glue database '{db_name}' deleted successfully")
        except Exception as e:
            self.log.warning(f"Error deleting Glue database '{db_name}': {e}")
        
        # Clean up Glue connection
        conn_name = prm["glue_connection"]["connection_name"]
        try:
            self.log.info(f"Deleting Glue connection '{conn_name}'...")
            glue.delete_connection(ConnectionName=conn_name)
            self.log.info(f"Glue connection '{conn_name}' deleted successfully")
        except Exception as e:
            self.log.warning(f"Error deleting Glue connection '{conn_name}': {e}")
        
        self.log.info("Glue resources cleanup completed")
    
    def _cleanup_s3(self):
        """Clean up S3 resources."""
        s3 = self.clients["s3"]
        prm = self.cfg["params"]
        
        # Extract bucket names from various S3 paths in the configuration
        buckets_to_clean = set()
        
        # From Glue ETL job script location
        if "glue_etl_job" in prm and "script_location" in prm["glue_etl_job"]:
            s3_path = prm["glue_etl_job"]["script_location"]
            if s3_path.startswith("s3://"):
                bucket = s3_path.replace("s3://", "").split("/")[0]
                buckets_to_clean.add(bucket)
        
        # From Athena output location
        if "athena" in prm and "output_location" in prm["athena"]:
            s3_path = prm["athena"]["output_location"]
            if s3_path.startswith("s3://"):
                bucket = s3_path.replace("s3://", "").split("/")[0]
                buckets_to_clean.add(bucket)
        
        # From TARGET_S3_LOCATION in Glue job arguments
        if "glue_etl_job" in prm and "default_arguments" in prm["glue_etl_job"] and "--TARGET_S3_LOCATION" in prm["glue_etl_job"]["default_arguments"]:
            s3_path = prm["glue_etl_job"]["default_arguments"]["--TARGET_S3_LOCATION"]
            if s3_path.startswith("s3://"):
                bucket = s3_path.replace("s3://", "").split("/")[0]
                buckets_to_clean.add(bucket)
        
        # Clean up each bucket
        for bucket in buckets_to_clean:
            try:
                self.log.info(f"Cleaning up objects in S3 bucket '{bucket}'...")
                
                # List and delete all objects in the bucket
                paginator = s3.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket):
                    if 'Contents' in page:
                        objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                        if objects_to_delete:
                            s3.delete_objects(
                                Bucket=bucket,
                                Delete={'Objects': objects_to_delete}
                            )
                            self.log.info(f"Deleted {len(objects_to_delete)} objects from bucket '{bucket}'")
                
                # Note: We're not deleting the bucket itself as it might be used for other purposes
                self.log.info(f"Cleaned up S3 bucket '{bucket}' (bucket not deleted)")
            except Exception as e:
                self.log.warning(f"Error cleaning up S3 bucket '{bucket}': {e}")
        
        self.log.info("S3 resources cleanup completed")
    
    def _cleanup_aurora(self):
        """Clean up Aurora resources."""
        rds = self.clients["rds"]
        prm = self.cfg["params"]
        cluster_id = prm["aurora"]["db_cluster_identifier"]
        
        try:
            # First, delete the DB instance
            instance_id = f"{cluster_id}-instance-1"  # Assuming this naming convention from the creation script
            try:
                self.log.info(f"Deleting Aurora DB instance '{instance_id}'...")
                rds.delete_db_instance(
                    DBInstanceIdentifier=instance_id,
                    SkipFinalSnapshot=True,
                    DeleteAutomatedBackups=True
                )
                self.log.info(f"Waiting for Aurora DB instance '{instance_id}' to be deleted...")
                
                # Wait for instance deletion to complete
                waiter = rds.get_waiter('db_instance_deleted')
                waiter.wait(DBInstanceIdentifier=instance_id)
                self.log.info(f"Aurora DB instance '{instance_id}' deleted successfully")
            except Exception as e:
                self.log.warning(f"Error deleting Aurora DB instance '{instance_id}': {e}")
            
            # Then, delete the DB cluster
            try:
                self.log.info(f"Deleting Aurora DB cluster '{cluster_id}'...")
                rds.delete_db_cluster(
                    DBClusterIdentifier=cluster_id,
                    SkipFinalSnapshot=True
                )
                self.log.info(f"Waiting for Aurora DB cluster '{cluster_id}' to be deleted...")
                
                # Wait for cluster deletion to complete
                waiter = rds.get_waiter('db_cluster_deleted')
                waiter.wait(DBClusterIdentifier=cluster_id)
                self.log.info(f"Aurora DB cluster '{cluster_id}' deleted successfully")
            except Exception as e:
                self.log.warning(f"Error deleting Aurora DB cluster '{cluster_id}': {e}")
            
            # Finally, delete the DB subnet group
            subnet_group_name = prm["aurora"]["db_subnet_group_name"]
            try:
                self.log.info(f"Deleting DB subnet group '{subnet_group_name}'...")
                rds.delete_db_subnet_group(DBSubnetGroupName=subnet_group_name)
                self.log.info(f"DB subnet group '{subnet_group_name}' deleted successfully")
            except Exception as e:
                self.log.warning(f"Error deleting DB subnet group '{subnet_group_name}': {e}")
            
            self.log.info("Aurora resources cleanup completed")
        except Exception as e:
            self.log.warning(f"Error during Aurora cleanup: {e}")
    
    def _cleanup_iam(self):
        """Clean up IAM resources."""
        iam = self.clients["iam"]
        prm = self.cfg["params"]
        role_name = prm["iam_role"]["role_name"]
        
        try:
            # First, detach all policies from the role
            try:
                self.log.info(f"Detaching policies from IAM role '{role_name}'...")
                attached_policies = iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]
                
                for policy in attached_policies:
                    policy_arn = policy["PolicyArn"]
                    self.log.info(f"Detaching policy '{policy_arn}' from role '{role_name}'...")
                    iam.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
                    self.log.info(f"Policy '{policy_arn}' detached successfully")
            except Exception as e:
                self.log.warning(f"Error detaching policies from role '{role_name}': {e}")
            
            # Then, delete the role
            try:
                self.log.info(f"Deleting IAM role '{role_name}'...")
                iam.delete_role(RoleName=role_name)
                self.log.info(f"IAM role '{role_name}' deleted successfully")
            except Exception as e:
                self.log.warning(f"Error deleting IAM role '{role_name}': {e}")
            
            self.log.info("IAM resources cleanup completed")
        except Exception as e:
            self.log.warning(f"Error during IAM cleanup: {e}")
    
    def _cleanup_vpc_endpoints(self):
        """Clean up VPC endpoints."""
        ec2 = self.clients["ec2"]
        prm = self.cfg["params"]
        
        try:
            # Get VPC ID from Aurora configuration
            vpc_id = None
            if "aurora" in prm and "vpc_id" in prm["aurora"]:
                vpc_id = prm["aurora"]["vpc_id"]
            
            if not vpc_id:
                self.log.warning("No VPC ID found in configuration, skipping VPC endpoints cleanup")
                return
            
            # Find all VPC endpoints in the VPC
            self.log.info(f"Finding VPC endpoints in VPC '{vpc_id}'...")
            endpoints = ec2.describe_vpc_endpoints(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["VpcEndpoints"]
            
            # Delete each endpoint
            for endpoint in endpoints:
                endpoint_id = endpoint["VpcEndpointId"]
                service_name = endpoint["ServiceName"]
                
                # Only delete endpoints we might have created (Glue and S3)
                if "glue" in service_name or "s3" in service_name:
                    self.log.info(f"Deleting VPC endpoint '{endpoint_id}' for service '{service_name}'...")
                    try:
                        ec2.delete_vpc_endpoints(VpcEndpointIds=[endpoint_id])
                        self.log.info(f"VPC endpoint '{endpoint_id}' deleted successfully")
                    except Exception as e:
                        self.log.warning(f"Error deleting VPC endpoint '{endpoint_id}': {e}")
            
            self.log.info("VPC endpoints cleanup completed")
        except Exception as e:
            self.log.warning(f"Error during VPC endpoints cleanup: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  Main entry point
# ════════════════════════════════════════════════════════════════════════════
def main():
    """Main entry point."""
    try:
        cleanup = AuroraAWSlessCleanup()
        success = cleanup.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        LOGGER.error(f"Unhandled error: {e}")
        import traceback
        LOGGER.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
