Financial Data ETL Pipeline
This project implements an ETL (Extract, Transform, Load) pipeline for financial data, including cryptocurrency, S&P 500 stock data, and stock market articles. The pipeline fetches data from various sources, processes and cleans it, and loads the results into a PostgreSQL database. The project supports two configurations: manual script execution and an automated pipeline using Apache Airflow.

Project Structure
bash
Copy code
├── pipeline/
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extract_data.py       # Data extraction functions (API calls, web scraping)
│   │   ├── transform_data.py     # Data transformation functions (cleaning, processing)
│   │   ├── load_data.py          # Data loading functions (inserting into PostgreSQL)
│   ├── logs/
│       ├── code_log.txt
├── workflow/                     # Airflow DAG files
│   ├── __init__.py
│   ├── airflow_exc.py            # Airflow DAG for automating the pipeline
├── config/                       # Configuration files, API keys, etc.
├── data/
│   ├── *.csv                     # All data files
└── README.md                     # Project documentation
Features
Data Sources:
Cryptocurrency Data: Fetches historical cryptocurrency rates and transaction data.
S&P 500 Data: Extracts stock and index data for companies in the S&P 500.
Market Articles: Scrapes financial news articles related to stock market trends.
Data Transformation:
Cleans and processes raw data, handling missing values, formatting issues, and data aggregation.
Outputs the cleaned data as CSV files or inserts them directly into a PostgreSQL database.
Data Loading:
Loads transformed data into PostgreSQL tables.
Supports loading cryptocurrency data, S&P 500 company data, stock data, index data, and market news articles.
Airflow Integration:
Automates the ETL pipeline using an Apache Airflow DAG.
Defines tasks for extracting, transforming, and loading each dataset.
How to Run
1. Prerequisites
Python 3.8+
PostgreSQL
Apache Airflow
Docker (optional for containerization)
Pipenv (or your preferred environment manager)
Install dependencies:

bash
Copy code
pip install -r requirements.txt
Set up PostgreSQL, and create the necessary database and tables using the SQL scripts provided.

2. Environment Setup
Configure environment variables by creating a .env file in the root directory:

bash
Copy code
CRYPTO_API_ENDPOINT=<your_crypto_api_endpoint>
ARTICLES_LINK=<your_articles_url>
CRYPTO_FILEPATH=./data/raw/crypto.csv
SP500_FILEPATH=./data/raw/sp500.csv
SP500_INDEX_FILEPATH=./data/raw/sp500_index.csv
SP500_STOCKS_FILEPATH=./data/raw/sp500_stocks.csv
MVR_FILEPATH=./data/raw/mvr.csv
SCRAPED_ARTICLES_FILEPATH=./data/raw/articles.csv
TRANSFORMED_DATA_DIR=./data/transformed
3. Running the ETL Pipeline Manually
To run the ETL pipeline manually, execute the main Python script that orchestrates the extraction, transformation, and loading processes:

bash
Copy code
python pipeline/main.py
4. Running with Airflow
Start the Airflow web server and scheduler:

bash
Copy code
airflow webserver --port 8080
airflow scheduler
Place the Airflow DAG file (finance_etl_pipeline.py) in the dags/ directory of your Airflow setup.

Access the Airflow web UI at http://localhost:8080 and trigger the DAG.

5. Logs and Monitoring
Logs for both manual execution and Airflow tasks are saved in the logs/ directory. Each task's progress and errors are logged for debugging purposes.

Future Improvements
Add data validation and more advanced error handling.
Extend support for additional data sources (e.g., more financial datasets).
Set up continuous integration (CI) for automated testing and deployment.
License
This project is licensed under the MIT License. See the LICENSE file for details
