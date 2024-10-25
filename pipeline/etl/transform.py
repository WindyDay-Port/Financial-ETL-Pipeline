import os
import pandas as pd
import logging
from dotenv import load_dotenv

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


# Transforming crypto.csv
def process_crypto_data(crypto_file):
    crypto_df = None
    try:
        # Reading the file and convert it to DataFrame
        crypto_df = pd.read_csv(rf"{crypto_file}")

        # Melt the DataFrame
        crypto_df = pd.melt(
            crypto_df,
            id_vars=['success', 'terms', 'privacy', 'timestamp', 'target', 'historical', 'date'],
            var_name='currency',  # Name for the 'variable' column
            value_name='rate'     # Name for the 'value' column
        )
        log_progress("DataFrame was read and melted successfully.")

        # Drop unnecessary columns
        crypto_df = crypto_df.drop(columns=['terms', 'privacy', 'historical'])

        # Fix timestamp format
        crypto_df['timestamp'] = pd.to_datetime(crypto_df['timestamp'], unit='s')

        # Edit 'date' data type
        crypto_df['date'] = pd.to_datetime(crypto_df['date'])

        # Drop rows where success = False
        crypto_df = crypto_df[crypto_df['success'] != False]

        # Drop 'success' column
        crypto_df = crypto_df.drop(columns=['success'])

        # Reorder by descending date
        crypto_df = crypto_df.sort_values(by='date', ascending=False)

        # Daily return or percentage of change in 'rate'
        crypto_df = crypto_df.sort_values(by='date')
        crypto_df['daily_return'] = crypto_df['rate'].pct_change()  # Calculate percentage change
        crypto_df = crypto_df.dropna()  # Drop the first row as there is no previous day to compare

        log_progress(f"Success: Transformation completed")

    except Exception as e:
        log_progress(f"Exception in transforming {crypto_file} pipeline: {e}")
        crypto_df = pd.DataFrame()

    finally:
        return crypto_df


# Transforming sp500_companies.csv
def transform_sp500_data(sp500_file):
    sp500_df = None
    try:
        sp500_df = pd.read_csv(rf"{sp500_file}")

        # Rename columns for consistency
        sp500_df = sp500_df.rename(columns={
            'Exchange': 'exchange',
            'Shortname': 'short_name',
            'Symbol': 'symbol',
            'Longname': 'long_name',
            'Sector': 'sector',
            'Industry': 'industry',
            'Currentprice': 'current_price',
            'Marketcap': 'market_cap',
            'Ebitda': 'ebitda',
            'Revenuegrowth': 'revenue_growth',
            'City': 'city',
            'State': 'state',
            'Country': 'country',
            'Fulltimeemployees': 'full_time_employees',
            'Longbusinesssummary': 'long_business_summary',
            'Weight': 'weight',
        })

        # Fill missing values with 0
        sp500_df = sp500_df.fillna(0)

        # Convert data types
        sp500_df['market_cap'] = sp500_df['market_cap'].astype(float)
        sp500_df['full_time_employees'] = sp500_df['full_time_employees'].astype(int)

        log_progress(f"Success: Transformation completed")
    except Exception as e:
        log_progress(f"Exception in transforming {sp500_file} pipeline: {e}")
        sp500_df = pd.DataFrame()

    finally:
        return sp500_df


# Transforming sp500_index.csv
def transform_sp500_index_data(sp500_index_file):
    sp500_index_df = None
    try:
        sp500_index_df = pd.read_csv(rf"{sp500_index_file}")

        # Renaming columns
        sp500_index_df = sp500_index_df.rename(columns={'Date': 'date', 'S&P500': 'S&P500_index_value'})

        log_progress(f"Success: Transformation completed")

    except Exception as e:
        log_progress(f"Exception in transforming {sp500_index_file} pipeline: {e}")
        sp500_index_df = pd.DataFrame()

    finally:
        return sp500_index_df


# Transforming sp500_stocks.csv
def transform_sp500_stock_data(sp500_stock_file):
    sp500_stock_df = None
    try:
        sp500_stock_df = pd.read_csv(rf"{sp500_stock_file}")

        sp500_stock_df = sp500_stock_df.rename(columns={
            'Date': 'date',
            'Symbol': 'comp_symbol',
            'Adj Close': 'adj_close',
            'Close': 'close_price',
            'High': 'maximum_value',
            'Low': 'minimum_value',
            'Open': 'opening_price',
            'Volume': 'traded_volume'
        })

        log_progress(f"Success: Transformation completed")

    except Exception as e:
        log_progress(f"Exception in transforming {sp500_stock_file} pipeline: {e}")
        sp500_stock_df = pd.DataFrame()

    finally:
        return sp500_stock_df


# Transforming MRV.csv
def transform_mvr_data(mvr_file):
    mastercard_df = None
    visa_df = None
    try:
        mvr_df = pd.read_csv(rf"{mvr_file}")

        mvr_df = mvr_df.rename(columns={
            'Date': 'date',
            'Open_M': 'mastercard_open_price',
            'High_M': 'mastercard_high_price',
            'Low_M': 'mastercard_low_price',
            'Close_M': 'mastercard_closing_price',
            'Adj Close_M': 'mastercard_adjusted_closing_price',
            'Volume_M': 'mastercard_trading_volume',
            'Open_V': 'visa_open_price',
            'High_V': 'visa_high_price',
            'Low_V': 'visa_low_price',
            'Close_V': 'visa_closing_price',
            'Adj Close_V': 'visa_adjusted_closing_price',
            'Volume_V': 'visa_trading_volume'
        })

        # Creating DataFrame for Mastercard
        mastercard_df = mvr_df[['date', 'mastercard_open_price', 'mastercard_high_price',
                                'mastercard_low_price', 'mastercard_closing_price',
                                'mastercard_adjusted_closing_price', 'mastercard_trading_volume']].copy()

        # Add a 'company' column to identify Mastercard
        mastercard_df['company'] = 'Mastercard'

        log_progress("Creating Mastercard DataFrame completed")

        # Creating DataFrame for Visa
        visa_df = mvr_df[['date', 'visa_open_price', 'visa_high_price',
                          'visa_low_price', 'visa_closing_price',
                          'visa_adjusted_closing_price', 'visa_trading_volume']].copy()

        # Add a 'company' column to identify Visa
        visa_df['company'] = 'Visa'

        log_progress("Creating Visa DataFrame completed")

        log_progress(f"Success: Splitting and transforming completed")

    except Exception as e:
        log_progress(f"Exception in transforming {mvr_file} pipeline: {e}")
        mastercard_df = pd.DataFrame()
        visa_df = pd.DataFrame()

    finally:
        return mastercard_df, visa_df


# Transform scraped_articles.csv
def transform_scraped_articles(scraped_articles_file):
    scraped_articles_df = None
    try:
        scraped_articles_df = pd.read_csv(rf'{scraped_articles_file}')

        scraped_articles_df = scraped_articles_df.rename(columns={
            'Title': 'title',
            'Link': 'link'
        })

        log_progress(f"Success: Transformation completed")

    except Exception as e:
        log_progress(f"Exception in transforming {scraped_articles_file} pipeline: {e}")
        scraped_articles_df = pd.DataFrame()

    finally:
        return scraped_articles_df
