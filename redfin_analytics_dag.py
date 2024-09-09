from airflow import DAG
from datetime import timedelta, datetime
import pandas as pd
from airflow.operators.python import PythonOperator
import boto3
from airflow.operators.bash import BashOperator


s3_client = boto3.client('s3')
target_bucket_name = 'redfin-transform-ali-yml '

data_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'

def extract_data(**kwargs):
    url = kwargs['url']
    df = pd.read_csv(url, compression='gzip', sep='\t')
    now = datetime.now()
    date_now_string = now.strftime("%d%m%Y%H%M%S")
    file_str = 'redfin_data_' + date_now_string
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    df.to_csv(output_file_path, index=False)
    output_list = [output_file_path, file_str]
    
    # Debugging print statement
    print("Extracted Data:", output_list)
    
    return output_list

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 19),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

def transform_data(task_instance, **kwargs):
    # Attempting to pull data from XCom
    data = task_instance.xcom_pull(task_ids='tsk_extract_redfin_data')
    
    # Debugging print statement
    print("Pulled XCom Data:", data)
    
    if data:
        data = data[0]
        object_key = task_instance.xcom_pull(task_ids='tsk_extract_redfin_data')[1]
        df = pd.read_csv(data)

        # Remove anomalies (commas) from 'city' column
        df['city'] = df['city'].str.replace(',', '')
        cols = [
            'period_begin', 'period_end', 'period_duration', 'region_type', 'region_type_id',
            'table_id', 'is_seasonally_adjusted', 'city', 'state', 'state_code', 'property_type',
            'property_type_id', 'median_sale_price', 'median_list_price', 'median_ppsf', 'median_list_ppsf',
            'homes_sold', 'inventory', 'months_of_supply', 'median_dom', 'avg_sale_to_list',
            'sold_above_list', 'parent_metro_region_metro_code', 'last_updated'
        ]

        df = df[cols]
        df = df.dropna()

        # Change the period_begin and period_end to datetime objects and extract years and months.
        df['period_begin'] = pd.to_datetime(df['period_begin'])
        df['period_end'] = pd.to_datetime(df['period_end'])

        df['period_begin_in_years'] = df['period_begin'].dt.year
        df['period_end_in_years'] = df['period_end'].dt.year

        df['period_begin_in_months'] = df['period_begin'].dt.month
        df['period_end_in_months'] = df['period_end'].dt.month

        # Map month number to respective month names
        month_dict = {
            "period_begin_in_months": {
                1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
            },
            "period_end_in_months": {
                1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
            }
        }

        df = df.replace(month_dict)
        print('Num of rows:', len(df))
        print('Num of cols:', len(df.columns))

        # Convert dataframe to CSV
        csv_data = df.to_csv(index=False)

        # Upload CSV to S3
        object_key = f"{object_key}.csv"
        s3_client.put_object(Bucket=target_bucket_name, Key=object_key, Body=csv_data)
    else:
        print("No data found in XCom for task 'tsk_extract_redfin_data'")

with DAG(
    'redfin_analytics_dag',
    default_args=default_args,
    # schedule_interval='@weekly',
    catchup=False
) as dag:

    extract_redfin_data = PythonOperator(
        task_id='tsk_extract_redfin_data',
        python_callable=extract_data,
        op_kwargs={'url': data_url}
    )

    transform_redfin_data = PythonOperator(
        task_id='tsk_transform_redfin_data',
        python_callable=transform_data,
        provide_context=True  # Ensures task_instance is passed automatically
    )

    load_to_s3 = BashOperator(
        task_id='tsk_load_to_s3',
        bash_command='aws s3 mv {{ ti.xcom_pull("tsk_extract_redfin_data")[0]}} s3://raw-data--redfin-storage-yml',
    )

    extract_redfin_data >> transform_redfin_data >> load_to_s3
