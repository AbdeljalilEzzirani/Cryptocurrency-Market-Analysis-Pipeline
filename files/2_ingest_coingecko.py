#!/usr/bin/env python3
import requests
import json
import subprocess
from datetime import datetime, timedelta

# Airflow imports (only used when running as a DAG)
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    airflow_available = True
except ImportError:
    airflow_available = False

def fetch_coingecko_data(**context):
    """Fetch top 100 cryptocurrencies from CoinGecko API."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 100,
        'page': 1,
        'sparkline': 'false'
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}")
    data = response.json()
    # Push to XCom if running in Airflow
    if context.get('ti'):
        context['ti'].xcom_push(key='raw_data', value=data)
    return data

def store_in_hdfs(data=None, execution_date=None, **context):
    """Store raw JSON data in HDFS with date partitioning.
    
    Args:
        data (dict, optional): JSON data from CoinGecko API. If None, pulled from XCom.
        execution_date (datetime or str, optional): Date for partitioning. Defaults to today if None.
        **context: Airflow context for XCom and execution date.
    """
    # If data isn’t provided (Airflow case), pull from XCom
    if data is None and context.get('ti'):
        data = context['ti'].xcom_pull(key='raw_data')
    if not data:
        raise ValueError("No data provided to store in HDFS")

    # Use execution_date from context['ds'] (Airflow) or parameter, default to today
    if context.get('ds'):
        execution_date = datetime.strptime(context['ds'], "%Y-%m-%d")
    elif isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
    else:
        execution_date = execution_date or datetime.now()

    # Convert data to JSON string
    json_data = json.dumps(data)
    local_file = "/tmp/coingecko_raw.json"
    with open(local_file, 'w') as f:
        f.write(json_data)
    
    # Partition path (e.g., YYYY=2025/MM=02/DD=25)
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")
    hdfs_dir = f"/user/etudiant/crypto/raw/YYYY={year}/MM={month}/DD={day}"
    hdfs_file = f"{hdfs_dir}/coingecko_raw.json"
    
    try:
        # Create directory and upload file
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_file], check=True)
        print(f"Stored data in {hdfs_file}")
    except subprocess.CalledProcessError as e:
        raise Exception(f"HDFS operation failed: {e}")

# Standalone execution
if __name__ == "__main__":
    try:
        data = fetch_coingecko_data(context={})  # Empty context for standalone
        store_in_hdfs(data=data, execution_date=None, context={})
    except Exception as e:
        print(f"Error: {e}")
        exit(1)

# Airflow DAG definition (only if Airflow is available)
if airflow_available:
    default_args = {
        'owner': 'etudiant',
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    with DAG(
        'coingecko_ingestion_dag',
        default_args=default_args,
        schedule_interval='@daily'
    ) as dag:
        fetch_data = PythonOperator(
            task_id='fetch_data',
            python_callable=fetch_coingecko_data,
            provide_context=True
        )
        store_raw_data = PythonOperator(
            task_id='store_raw_data_in_hdfs',
            python_callable=store_in_hdfs,
            provide_context=True
        )
        fetch_data >> store_raw_data


# #!/usr/bin/env python3
# import requests
# import json
# import subprocess
# from datetime import datetime

# def fetch_coingecko_data():
#     url = "https://api.coingecko.com/api/v3/coins/markets"
#     params = {
#         'vs_currency': 'usd',
#         'order': 'market_cap_desc',
#         'per_page': '100',
#         'page': '1',
#         'sparkline': 'false'
#     }
#     response = requests.get(url, params=params)
#     if response.status_code != 200:
#         raise Exception(f"API call failed: {response.status_code}")
#     return response.json()

# def store_in_hdfs(data):
#     # Use today’s date for partitioning
#     today = datetime.now()
#     year = today.strftime("%Y")
#     month = today.strftime("%m")
#     day = today.strftime("%d")
    
#     # Write JSON locally
#     json_data = json.dumps(data)
#     local_file = "/tmp/coingecko_raw.json"
#     with open(local_file, "w") as f:
#         f.write(json_data)
    
#     # HDFS path
#     hdfs_dir = f"/user/etudiant/crypto/raw/YYYY={year}/MM={month}/DD={day}"
#     hdfs_file = f"{hdfs_dir}/coingecko_raw.json"
    
#     # Store in HDFS
#     subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
#     subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_file], check=True)
#     print(f"Data stored in {hdfs_file}")

# if __name__ == "__main__":
#     data = fetch_coingecko_data()
#     store_in_hdfs(data)