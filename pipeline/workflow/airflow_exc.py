from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Import ETL scripts
from pipeline.etl.extract import (
    fetch_data_from_api,
    scraping_websites
)
from pipeline.etl.transform import (
    process_crypto_data,
    transform_sp500_data,
    transform_sp500_index_data,
    transform_sp500_stock_data,
    transform_mvr_data,
    transform_scraped_articles
)
from pipeline.etl.load import (
    insert_sp500_company,
    insert_sp500_index,
    insert_crypto,
    insert_sp500_stock,
    insert_articles,
    insert_mastercard_stock,
    insert_visa_stock
)

# Load environment variables
load_dotenv()

# File paths from environment variables
crypto_data_file = os.getenv("CRYPTO_FILEPATH")
mvr_data_file = os.getenv("MVR_FILEPATH")
sp500_data_file = os.getenv("SP500_FILEPATH")
sp500_index_data_file = os.getenv("SP500_INDEX_FILEPATH")
sp500_stocks_data_file = os.getenv("SP500_STOCKS_FILEPATH")
scraped_articles_data_file = os.getenv("SCRAPED_ARTICLES_FILEPATH")

# Directory to save transformed files
transformed_dir = os.getenv("TRANSFORMED_DATA_DIR")

# DAG default arguments
default_args = {
    'owner': 'Hau_Nguyen',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 31),
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Define the DAG
with DAG(
    'finance_etl_pipeline',
    default_args=default_args,
    description='An ETL pipeline using Airflow',
    schedule_interval=timedelta(days=1),  # Daily run
) as dag:

    # Paths for transformed data
    transformed_crypto_file = os.path.join(transformed_dir, "crypto_transformed.csv")
    transformed_sp500_file = os.path.join(transformed_dir, "sp500_transformed.csv")
    transformed_sp500_index_file = os.path.join(transformed_dir, "sp500_index_transformed.csv")
    transformed_sp500_stocks_file = os.path.join(transformed_dir, "sp500_stocks_transformed.csv")
    transformed_mvr_file = os.path.join(transformed_dir, "mvr_transformed.csv")
    transformed_articles_file = os.path.join(transformed_dir, "articles_transformed.csv")

    # Extract Tasks
    crypto_extract_task = PythonOperator(
        task_id='extract_crypto_data',
        python_callable=fetch_data_from_api,
        op_kwargs={'api_endpoint': os.getenv('CRYPTO_API_ENDPOINT')},
    )

    scraping_article_task = PythonOperator(
        task_id='extract_articles',
        python_callable=scraping_websites,
        op_kwargs={'articles_link': os.getenv('ARTICLES_LINK')},
    )

    # Transformation Tasks
    transform_crypto_task = PythonOperator(
        task_id='process_crypto_data',
        python_callable=process_crypto_data,
        op_kwargs={'crypto_file': crypto_data_file, 'output_file': transformed_crypto_file},
    )

    transform_sp500_comp_task = PythonOperator(
        task_id='process_sp500_data',
        python_callable=transform_sp500_data,
        op_kwargs={'sp500_file': sp500_data_file, 'output_file': transformed_sp500_file},
    )

    transform_sp500_index_task = PythonOperator(
        task_id='process_sp500_index_data',
        python_callable=transform_sp500_index_data,
        op_kwargs={'sp500_index_file': sp500_index_data_file, 'output_file': transformed_sp500_index_file},
    )

    transform_sp500_stock_task = PythonOperator(
        task_id='process_sp500_stock_data',
        python_callable=transform_sp500_stock_data,
        op_kwargs={'sp500_stocks_file': sp500_stocks_data_file, 'output_file': transformed_sp500_stocks_file},
    )

    transform_mvr_task = PythonOperator(
        task_id='process_mvr_data',
        python_callable=transform_mvr_data,
        op_kwargs={'mvr_file': mvr_data_file, 'output_file': transformed_mvr_file},
    )

    transform_scraped_article = PythonOperator(
        task_id='process_scraped_articles',
        python_callable=transform_scraped_articles,
        op_kwargs={'scraped_articles_file': scraped_articles_data_file, 'output_file': transformed_articles_file},
    )

    # Load Task
    load_crypto_task = PythonOperator(
        task_id='load_crypto_data',
        python_callable=insert_crypto,
        op_kwargs={'crypto_file': transformed_crypto_file},
    )

    load_sp500_company_task = PythonOperator(
        task_id='load_sp500_company_data',
        python_callable=insert_sp500_company,
        op_kwargs={'sp500_file': transformed_sp500_file},
    )

    load_sp500_index_task = PythonOperator(
        task_id='load_sp500_index_data',
        python_callable=insert_sp500_index,
        op_kwargs={'sp500_index_file': transformed_sp500_index_file},
    )

    load_sp500_stock_task = PythonOperator(
        task_id='load_sp500_stock_data',
        python_callable=insert_sp500_stock,
        op_kwargs={'sp500_stocks_file': transformed_sp500_stocks_file},
    )

    load_mvr_mastercard_task = PythonOperator(
        task_id='load_mastercard_data',
        python_callable=insert_mastercard_stock,
        op_kwargs={'mvr_file': transformed_mvr_file},
    )

    load_mvr_visa_task = PythonOperator(
        task_id='load_visa_data',
        python_callable=insert_visa_stock,
        op_kwargs={'mvr_file': transformed_mvr_file},
    )

    load_scraped_articles_task = PythonOperator(
        task_id='load_scraped_articles',
        python_callable=insert_articles,
        op_kwargs={'scraped_articles_file': transformed_articles_file},
    )

    # Task Dependencies

    # Extract -> Transform -> Load for crypto data
    crypto_extract_task >> transform_crypto_task >> load_crypto_task

    # Extract -> Transform -> Load for scraped articles
    scraping_article_task >> transform_scraped_article >> load_scraped_articles_task

    # S&P 500 company data
    transform_sp500_comp_task >> load_sp500_company_task

    # S&P 500 index data
    transform_sp500_index_task >> load_sp500_index_task

    # S&P 500 stock data
    transform_sp500_stock_task >> load_sp500_stock_task

    # MVR (Mastercard and Visa stock data)
    transform_mvr_task >> [load_mvr_mastercard_task, load_mvr_visa_task]
