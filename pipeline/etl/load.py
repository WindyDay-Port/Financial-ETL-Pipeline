import psycopg2
import logging
import os
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


# Inserting data into sp500_company table:
def insert_sp500_company(dataframe):
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        log_progress("Database connection established successfully.")

        cursor_object = connection.cursor()

        for index, row in dataframe.iterrows():
            query = """
            INSERT INTO sp500_company (exchange, symbol, short_name, long_name, sector, industry,
                                   current_stock_price, current_marketcap, ebitda, revenue_growth,
                                   city, state, country, full_time_emp, business_summary, weight)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO NOTHING;
            """
            cursor_object.execute(query, tuple(row))

        connection.commit()

        cursor_object.close()

        connection.close()

        log_progress("Data was loaded without any proplem")

    except Exception as e:
        log_progress(f"Exception in loading data: {e}")

    finally:
        log_progress("Loading process has completed successfully. Connection is closed")


# Inserting data into sp500_index table:
def insert_sp500_index(dataframe):
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        log_progress("Database connection established successfully.")

        cursor_object = connection.cursor()

        log_progress("Data was read successfully. Initializing loading process...")

        for index, row in dataframe.iterrows():
            query = """
            INSERT INTO sp500_index_table (date, sp500_index_value)
            VALUES (%s, %s)
            ON CONFLICT (date) DO NOTHING;
            """
            cursor_object.execute(query, tuple(row))

        connection.commit()

        cursor_object.close()

        connection.close()

        log_progress("Data was loaded without any proplem")

    except Exception as e:
        log_progress(f"Exception in loading data: {e}")

    finally:
        log_progress("Loading process has completed successfully. Connection is closed")


# Inserting data into sp500_stock table:
def insert_sp500_stock(dataframe):
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        log_progress("Database connection established successfully.")

        cursor_object = connection.cursor()

        for index, row in dataframe.iterrows():
            query = """
            INSERT INTO sp500_stock_table (date, comp_symbol, adj_close, close_price, maximum_value,
                                    minimum_value, opening_price, traded_volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor_object.execute(query, tuple(row))

        connection.commit()

        cursor_object.close()

        connection.close()

        log_progress("Data was loaded without any problem.")

    except Exception as e:
        log_progress(f"Exception in loading data: {e}")

    finally:
        log_progress("Loading process has completed successfully. Connection is closed")


# Inserting data into crypto table:
def insert_crypto(dataframe):
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        log_progress("Database connection established successfully.")

        cursor_object = connection.cursor()

        for index, row in dataframe.iterrows():
            query = """
            INSERT INTO crypto_table (time_stamp, target, date, currency, rate, daily_return)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            cursor_object.execute(query, tuple(row))

        connection.commit()

        cursor_object.close()

        connection.close()

        log_progress("Data was loaded without any problem")

    except Exception as e:
        log_progress(f"Exception in loading data: {e}")

    finally:
        log_progress("Loading process has completed successfully. Connection is closed")


# Inserting data into visa_stock table:
def insert_visa_stock(dataframe):
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        log_progress("Database connection established successfully.")

        cursor_object = connection.cursor()

        log_progress("Data was read successfully. Initializing loading process...")

        for index, row in dataframe.iterrows():
            query = """
            INSERT INTO visa_stock_table (date, open_price, high_price, low_price, closing_price,
                                       adj_closing_price, trading_volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """
            cursor_object.execute(query, tuple(row))

        connection.commit()

        cursor_object.close()

        connection.close()

        log_progress("Data was loaded without any problem")

    except Exception as e:
        log_progress(f"Exception in loading data: {e}")

    finally:
        log_progress("Loading process has completed successfully. Connection is closed")


# Inserting data into Mastercard_stock table
def insert_mastercard_stock(dataframe):
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        log_progress("Database connection established successfully.")

        cursor_object = connection.cursor()

        log_progress("Data was read successfully. Initializing loading process...")

        for index, row in dataframe.iterrows():
            query = """
            INSERT INTO mastercard_stock_table (date, open_price, high_price, low_price, closing_price,
                                         adj_closing_price, trading_volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """
            cursor_object.execute(query, tuple(row))

        connection.commit()

        cursor_object.close()

        connection.close()

        log_progress("Data was loaded without any problem")

    except Exception as e:
        log_progress(f"Exception in loading data: {e}")

    finally:
        log_progress("Loading process has completed successfully. Connection is closed")


# Inserting data into articles table:
def insert_articles(dataframe):
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        log_progress("Database connection established successfully.")

        cursor_object = connection.cursor()

        for index, row in dataframe.iterrows():
            query = """
            INSERT INTO articles_table (title, link)
            VALUES (%s, %s);
            """
            cursor_object.execute(query, tuple(row))

        connection.commit()

        cursor_object.close()

        connection.close()

        log_progress("Data was loaded without any problem")

    except Exception as e:
        log_progress(f"Exception in loading data: {e}")

    finally:
        log_progress("Loading process has completed successfully. Connection is closed")