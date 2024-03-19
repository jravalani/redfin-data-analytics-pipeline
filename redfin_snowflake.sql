-- Drop the database if it exists to avoid conflicts
DROP DATABASE IF EXISTS redfin_database;

-- Create a new database named 'redfin_database'
CREATE DATABASE redfin_database;

-- Create a schema named 'redfin_schema' within the 'redfin_database'
CREATE SCHEMA redfin_schema;

-- Create or replace the 'redfin_table' table within the 'redfin_schema'
-- Truncate the table to remove any existing data
TRUNCATE TABLE redfin_database.redfin_schema.redfin_table;
CREATE OR REPLACE TABLE redfin_database.redfin_schema.redfin_table (
    period_begin DATE,
    period_end DATE,
    period_duration INT,
    region_type STRING,
    region_type_id INT,
    table_id INT,
    is_seasonally_adjusted STRING,
    city STRING,
    state STRING,
    state_code STRING,
    property_type STRING,
    property_type_id INT,
    median_sale_price FLOAT,
    median_list_price FLOAT,
    median_ppsf FLOAT,
    median_list_ppsf FLOAT,
    homes_sold FLOAT,
    inventory FLOAT,
    months_of_supply FLOAT,
    median_dom FLOAT,
    avg_sale_to_list FLOAT,
    sold_above_list FLOAT,
    parent_metro_region_metro_code STRING,
    last_updated DATETIME,
    period_begin_in_years STRING,
    period_end_in_years STRING,
    period_begin_in_months STRING,
    period_end_in_months STRING
);

-- Select the first 10 records from the 'redfin_table'
SELECT *
FROM redfin_database.redfin_schema.redfin_table LIMIT 10;

-- Count the total number of records in the 'redfin_table'
SELECT COUNT(*) FROM redfin_database.redfin_schema.redfin_table;

-- Create a schema named 'file_format_schema' for file format objects
CREATE SCHEMA file_format_schema;

-- Create or replace a file format object named 'format_csv' for CSV files
CREATE OR REPLACE FILE FORMAT redfin_database.file_format_schema.format_csv
    TYPE = 'CSV'  -- Specify the file format as CSV
    FIELD_DELIMITER = ','  -- Define the field delimiter as comma
    RECORD_DELIMITER = '\n'  -- Specify the record delimiter as newline
    SKIP_HEADER = 1;  -- Skip the first row as it contains headers

-- Create a schema named 'external_stage_schema' for external stages
CREATE SCHEMA external_stage_schema;

-- Create or replace an external stage named 'redfin_ext_stage_yml'
CREATE OR REPLACE STAGE redfin_database.external_stage_schema.redfin_ext_stage_yml;

-- List files in the 'redfin_ext_stage_yml' stage
LIST @redfin_database.external_stage_schema.redfin_ext_stage_yml;

-- Create a schema named 'snowpipe_schema' for Snowpipe
CREATE SCHEMA snowpipe_schema;

-- Create or replace a Snowpipe named 'redfin_snowpipe' for auto ingestion
CREATE OR REPLACE PIPE redfin_database.snowpipe_schema.redfin_snowpipe
AUTO_INGEST = TRUE
AS 
COPY INTO redfin_database.redfin_schema.redfin_table
FROM @redfin_database.external_stage_schema.redfin_ext_stage_yml;

-- Describe the 'redfin_snowpipe' for information
DESC PIPE redfin_database.snowpipe_schema.redfin_snowpipe;

-- Select the first 10 records from the 'redfin_table' after ingestion
SELECT *
FROM redfin_database.redfin_schema.redfin_table LIMIT 10; 
