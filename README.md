# Redfin Data Analytics Pipeline

This repository contains scripts for setting up a data analytics pipeline for processing Redfin housing market data. The pipeline includes extraction of data from a source URL, transformation of the extracted data, loading the transformed data into an AWS S3 bucket, and visualizing it using Power BI.

## Scripts

### 1. Snowflake SQL Script

The Snowflake SQL script sets up a data pipeline in Snowflake for ingesting Redfin housing market data into a database. It includes creating the necessary database objects such as tables, file formats, external stages, and Snowpipe for continuous data ingestion.

Filename: `snowflake_data_pipeline.sql`

### 2. Airflow DAG Python Script

The Airflow DAG Python script defines an Apache Airflow Directed Acyclic Graph (DAG) for orchestrating the data pipeline. It consists of tasks for extracting data from a source URL, transforming the extracted data, and loading the transformed data into an AWS S3 bucket.

Filename: `redfin_analytics_dag.py`

## Usage

To use these scripts:

1. Ensure you have access to Snowflake and Apache Airflow.
2. Set up your Snowflake environment and execute the `snowflake_data_pipeline.sql` script to create the necessary database objects.
3. Install Apache Airflow on your Amazon EC2 instance and configure it to run the `redfin_analytics_dag.py` script.
4. Adjust the configurations in both scripts according to your environment, such as AWS S3 bucket names and database connection details.
5. Run the Airflow DAG to start the data pipeline execution.
6. Load the data from Snowflake into Power BI for visualization.

## Dependencies

- Snowflake
- Apache Airflow
- Power BI
- Python 3
- pandas
- boto3 (for S3 interaction in the Airflow DAG script)

## Contributors

- Jay Ravalani

