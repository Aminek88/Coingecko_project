from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import json
import subprocess
import happybase

# Default arguments for DAG
default_args = {
    'owner': 'amine8kkh',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_crypto_data(**context):
    """Récupère les données horaires des 3 cryptos depuis yfinance."""
    symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD']
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)
    
    all_data = []
    
    for symbol in symbols:
        try:
            crypto = yf.Ticker(symbol)
            df = crypto.history(
                start=start_time,
                end=end_time,
                interval='1h'
            )
            
            if df.empty:
                print(f"Aucune donnée pour {symbol}")
                continue
                
            df = df.reset_index()
            df = df.rename(columns={
                'Datetime': 'datetime',
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price',
                'Volume': 'volume'
            })
            df['coin'] = symbol.replace('-USD', '')
            
            columns_to_keep = ['datetime', 'open_price', 'high_price', 'low_price', 
                               'close_price', 'volume', 'coin']
            df = df[columns_to_keep]
            
            # Conversion des timestamps en chaînes ISO pour JSON
            df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            all_data.append(df)
            
        except Exception as e:
            print(f"Erreur lors de la récupération de {symbol}: {e}")
            continue
    
    if not all_data:
        raise ValueError("Aucune donnée récupérée depuis yfinance")
        
    # Combinaison des données
    final_df = pd.concat(all_data)
    
    # Conversion en liste de dictionnaires
    json_data = final_df.to_dict(orient='records')
    
    # Aperçu des données
    print(json.dumps(json_data[:5], indent=4))
    
    # Push dans XCom
    context['ti'].xcom_push(key='raw_data', value=json_data)

def store_data_in_hdfs(**context):
    data = context['ti'].xcom_pull(key='raw_data')
    json_data = json.dumps(data)
    local_file = 'tmp/crypto_raw.json'
    with open(local_file, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')  # Écrire chaque objet sur une nouvelle ligne
    
    execution_date = context['ds']
    year, month, day = execution_date.split('-')
    hdfs_dir = f"/user/etudiant/crypto/raw/YYYY={year}/MM={month}/DD={day}"
    hdfs_file_path = f"{hdfs_dir}/yfinance_raw.json"
    try:
        # Create the HDFS directory and upload the file
        subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_dir], check=True)
        subprocess.run(['hdfs', 'dfs', '-put', '-f', local_file, hdfs_file_path], check=True)
        context['ti'].xcom_push(key='stored_raw_data', value=json_data)
    except subprocess.CalledProcessError as e:
        raise Exception(f'HDFS operation failed: {e}')

def store_in_hbase(**context):
    # Retrieve the execution date (YYYY-MM-DD format)
    execution_date = context['ds']
    year, month, day = execution_date.split('-')

    # Define HDFS paths
    hdfs_raw_path = f"/user/etudiant/crypto/raw/YYYY={year}/MM={month}/DD={day}/yfinance_raw.json"
    hdfs_transformed_dir = f"/user/etudiant/crypto/procced/YYYY={year}/MM={month}/DD={day}"
    hdfs_transformed_path = f"{hdfs_transformed_dir}/transformed_data.json"  # Explicit filename

    # Read raw data from HDFS
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", hdfs_raw_path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise ValueError(f"Error reading {hdfs_raw_path}: {result.stderr}")
    
    # Transform raw JSON data into a Python dictionary
    lines = result.stdout.strip().split('\n')
    transformed_data = [json.loads(line) for line in lines if line.strip()]

    # Write transformed data back to HDFS
    transformed_json = "\n".join(json.dumps(record) for record in transformed_data)
    
    # Create the target HDFS directory if it doesn't exist
    mkdir_process = subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", hdfs_transformed_dir],
        capture_output=True, text=True
    )
    if mkdir_process.returncode != 0:
        raise ValueError(f"Error creating HDFS directory: {mkdir_process.stderr}")
    
    # Use `-put` with `-f` to overwrite if the file exists
    put_process = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", "-", hdfs_transformed_path],
        input=transformed_json, text=True, capture_output=True
    )
    if put_process.returncode != 0:
        raise ValueError(f"Error writing to {hdfs_transformed_path}: {put_process.stderr}")
    
    print(f"Transformed data successfully stored in {hdfs_transformed_path}")

    # Connect to HBase
    connection = happybase.Connection('localhost', port=8081)
    
    # Check for the existence of the 'crypto_prices' table and create it if necessary
    tables = connection.tables()
    if b'crypto_prices' not in tables:
        connection.create_table(
            'crypto_prices',
            {'stats': dict(max_versions=3)}
        )

    table = connection.table('crypto_prices')

    # Insert data into HBase in batches
    with table.batch() as batch:
        for record in transformed_data:
            coin = record['coin'].lower()  # Convert to lowercase (e.g., "BTC" -> "bitcoin")
            row_key = f"{coin}_{execution_date}"  # Generate the row key (e.g., "bitcoin_2025-02-27")
            batch.put(row_key, {
                b'stats:open_price': str(record['open_price']).encode('utf-8'),
                b'stats:high_price': str(record['high_price']).encode('utf-8'),
                b'stats:low_price': str(record['low_price']).encode('utf-8'),
                b'stats:close_price': str(record['close_price']).encode('utf-8'),
                b'stats:volume': str(record['volume']).encode('utf-8')
            })

    # Close the HBase connection
    connection.close()

# Define DAG
with DAG('crypto_ingestion_dag_3',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_crypto_data,
    )

    store_raw_data = PythonOperator(
        task_id='store_raw_data_in_hdfs',
        python_callable=store_data_in_hdfs,
    )

    store_in_hbase_task = PythonOperator(
        task_id='store_in_hbase',
        python_callable=store_in_hbase,
    )

    fetch_data >> store_raw_data >> store_in_hbase_task