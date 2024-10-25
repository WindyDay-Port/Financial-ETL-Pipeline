import os
import logging
from dotenv import load_dotenv


from pipeline.etl.extract import (
    fetch_data_from_api,
    scraping_websites)
from pipeline.etl.transform import (
    process_crypto_data,
    transform_sp500_data,
    transform_sp500_index_data,
    transform_sp500_stock_data,
    transform_mvr_data,
    transform_scraped_articles
)
from pipeline.etl.load import (
    insert_crypto,
    insert_articles,
    insert_sp500_company,
    insert_sp500_index,
    insert_sp500_stock,
    insert_visa_stock,
    insert_mastercard_stock
)

load_dotenv()


# Setting up logging
logger = logging.getLogger(__name__)


# Setting up logging to log in the logs/ directory
current_dir = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(current_dir, 'logs', 'code_log.txt')


logging.basicConfig(
    filename=log_file_path,  # Save the log file in logs/ directory
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def log_progress(message):
    logger.debug(message)


# Fetching and storing data as files in created directory
def extracted_data():
    crypto_api = os.getenv("CRYPTO_API_ENDPOINT")
    article_url = os.getenv("ARTICLES_LINK")

    retrieve_and_store_data = (fetch_data_from_api(crypto_api),
                               scraping_websites(article_url))

    log_progress("Completed extracting data")

    return retrieve_and_store_data


log_progress("Extract data completed successfully. Initializing transformation and loading process...")


# Transforming raw data files and load them into
def processed_and_load_data():
    article_data_file = os.getenv("SCRAPED_ARTICLES_FILEPATH")
    crypto_data_file = os.getenv("CRYPTO_FILEPATH")
    mvr_data_file = os.getenv("MRV_FILEPATH")
    sp500_data_file = os.getenv("SP500_FILEPATH")
    sp500_index_data_file = os.getenv("SP500_INDEX_FILEPATH")
    sp500_stocks_data_file = os.getenv("SP500_STOCKS_FILEPATH")

    crypto_df = process_crypto_data(crypto_data_file)
    insert_crypto(crypto_df)

    sp500_df = transform_sp500_data(sp500_data_file)
    insert_sp500_company(sp500_df)

    sp500_stock_df = transform_sp500_stock_data(sp500_stocks_data_file)
    insert_sp500_stock(sp500_stock_df)

    sp500_index_df = transform_sp500_index_data(sp500_index_data_file)
    insert_sp500_index(sp500_index_df)

    mastercard_df, visa_df = transform_mvr_data(mvr_data_file)
    insert_visa_stock(visa_df)
    insert_mastercard_stock(mastercard_df)

    scraped_articles_df = transform_scraped_articles(article_data_file)
    insert_articles(scraped_articles_df)

    log_progress("Transform and load data into PostgreSQL completed successfully")
