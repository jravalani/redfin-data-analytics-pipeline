from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3

# Initialize AWS S3 client
s3_client = boto3.client('s3')

# Target S3 bucket for storing transformed data
target_bucket_name = 'yours3bucketname'

# URL for data extraction
data_extraction_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'

def extract_data(**kwargs):
    """
    Extracts data from the specified URL, timestamps the extraction,
    saves it as a CSV file, and returns the output file path and a unique file string.
    """
    url = kwargs['url']
    df = pd.read_csv(url, compression='gzip', sep='\t')

    # Timestamp the extraction
    time_now = datetime.now()
    date_now_string = time_now.strftime("%d%m%Y%H%M%S")

    # Create a unique file string
    file_string = 'redfin_data_' + date_now_string

    # Save the DataFrame as a CSV file
    df.to_csv(f"{file_string}.csv", index=False)

    # Define the output file path
    output_file_path = f"/home/ubuntu/{file_string}.csv"

    # Return output information as a list
    return [output_file_path, file_string]

def transform_data(task_instance):
    """
    Transforms the extracted data, converts date columns,
    and uploads the transformed data to an S3 bucket.
    """
    # Retrieve output of the data extraction task using XCom
    data, file_string = task_instance.xcom_pull(task_ids="task_extract_redfin_data")

    # Read the extracted data into a Pandas DataFrame
    df = pd.read_csv(data)

    # Transformation Steps (Example):
    # Remove commas from the 'city' column
    df['city'] = df['city'].str.replace(',', '')

    # Select specific columns
    cols = ['period_begin', 'period_end', 'region_type', 'median_sale_price', 'homes_sold']
    df = df[cols]

    # Drop rows with missing values
    df = df.dropna()

    # Convert date columns to datetime objects
    df['period_begin'] = pd.to_datetime(df['period_begin'])
    df['period_end'] = pd.to_datetime(df['period_end'])

    # Extract years and months from date columns
    df["period_begin_year"] = df['period_begin'].dt.year
    df["period_end_year"] = df['period_end'].dt.year
    df["period_begin_month"] = df['period_begin'].dt.month
    df["period_end_month"] = df['period_end'].dt.month

    # Map month numbers to month names
    month_dict = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    df["period_begin_month"] = df["period_begin_month"].map(month_dict)
    df["period_end_month"] = df["period_end_month"].map(month_dict)

    # Display information about the DataFrame
    print('Number of rows:', len(df))
    print('Number of columns:', len(df.columns))

    # Convert DataFrame to CSV format
    csv_data = df.to_csv(index=False)

    # Upload CSV to S3
    file_string = f"{file_string}.csv"
    s3_client.put_object(Bucket=target_bucket_name, Key=file_string, Body=csv_data)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 14),
    'email': ['youmail@abc'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG('redfin_analytics_dag', default_args=default_args, catchup=False) as dag:

    # Task to extract Redfin data
    extract_redfin_data = PythonOperator(
        task_id='task_extract_redfin_data',
        python_callable=extract_data,
        op_kwargs={'url': data_extraction_url},
        provide_context=True  # Provide context to the PythonOperator
    )

    # Task to transform Redfin data
    transform_redfin_data = PythonOperator(
        task_id='task_transform_redfin_data',
        python_callable=transform_data
    )

    # Task to load transformed data to S3
    load_to_s3 = BashOperator(
        task_id='task_load_to_s3',
        bash_command='aws s3 mv {{ ti.xcom_pull("task_extract_redfin_data")[0]}} s3://rawdatas3bucket'
    )

    # Set task dependencies
    extract_redfin_data >> transform_redfin_data >> load_to_s3
