# Aurora-Athena ETL Pipeline Using AWS Glue

## Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline that:

1. Creates an Aurora MySQL database cluster
2. Loads sample data into the database
3. Uses AWS Glue to catalog the database
4. Transforms the data to Parquet format and stores it in S3
5. Makes the data queryable through Amazon Athena

The pipeline demonstrates a complete data engineering workflow using AWS services to create a data lake architecture.

## Key Components

### Aurora MySQL Setup
- Automatically provisions an Aurora MySQL cluster
- Creates necessary subnet groups and security configurations
- Loads sample data schemas including employee and customer information

### AWS Glue Integration
- Creates Glue connections to the Aurora database
- Sets up Glue crawlers to catalog database schemas
- Implements Glue ETL jobs to transform data to Parquet format

### Amazon Athena Query Interface
- Configures Athena to query the transformed data
- Sets up output locations for query results
- Provides example queries for data analysis

### Resource Management
- Includes cleanup utilities to remove all created AWS resources
- Implements proper IAM roles and permissions

## Scripts

### Core Pipeline
- `aurora_athena_etl_pipeline.py`: Main ETL orchestration script that sets up the entire pipeline
- `aurora_athena_etl_pipeline_db.py`: Database-specific operations for the ETL pipeline

### Data Export
- `export_employee_tables_to_athena.py`: Specialized script for exporting employee tables to Athena

### Database Operations
- `query_aurora.py`: Utility for querying the Aurora MySQL database
- `reset_employee_tables.py`: Script to reset and reload employee tables

### Resource Management
- `aurora_awsless.py`: Cleanup script to remove all AWS resources created by the pipeline

## Database Schema

The project works with two main database schemas:

1. **Customer Orders Schema**:
   - `customers`: Customer information
   - `orders`: Order details linked to customers

2. **Employee Database Schema** (as documented in `mig-db.md`):
   - `employees`: Employee information
   - `departments`: Department details
   - `dept_manager`: Department manager assignments
   - `dept_emp`: Department employee assignments
   - `titles`: Employee job titles
   - `salaries`: Employee salary information

## Configuration

The pipeline uses two main configuration sources:

1. `.env` file (in the parent `bin` directory) containing:
   - AWS credentials
   - Region settings
   - Aurora database password

2. `parameters.json` file (in the parent `bin` directory) containing:
   - Aurora cluster configuration
   - Glue job settings
   - IAM role definitions
   - S3 bucket configurations
   - Athena settings

## Prerequisites

- AWS account with appropriate permissions
- Python 3.6+
- Required Python packages: boto3, pymysql, dotenv
- AWS CLI configured with appropriate credentials

## Usage

### Setting Up the Pipeline

1. Configure your AWS credentials in the `.env` file
2. Adjust parameters in `parameters.json` as needed
3. Run the main pipeline script:
   ```
   python aurora_athena_etl_pipeline.py
   ```

### Exporting Employee Tables

```
python export_employee_tables_to_athena.py
```

### Querying Aurora Database

```
python query_aurora.py
```

### Cleaning Up Resources

```
python aurora_awsless.py
```

## Security Considerations

- The pipeline creates IAM roles with least privilege permissions
- Database credentials are stored in environment variables
- Security groups are configured for appropriate access

## Troubleshooting

- Check the log files (`aurora_athena_etl.log` and `aurora_awsless.log`) for detailed error information
- Ensure your AWS credentials have the necessary permissions
- Verify network connectivity to AWS services

## License

This project is part of the AWS Data Engineering Academy training materials.
