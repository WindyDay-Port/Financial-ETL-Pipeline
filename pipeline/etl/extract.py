# Required modules and libraries
import os
import requests
import pandas as pd
import logging
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()


# Setting up logging
logger = logging.getLogger(__name__)


# Setting up logging to log in the logs/ directory
current_dir = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(current_dir, 'logs', 'code_log.txt')


# Set up logging to log in the logs/ directory
logging.basicConfig(
    filename=log_file_path,  # Save the log file in logs/ directory
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def log_progress(message):
    logger.debug(message)


# Extracting data from API and converting it into .CSV format
def fetch_data_from_api(api_endpoint):
    api_status = None
    try:
        response = requests.get(api_endpoint)
        api_status = response.status_code
        if api_status == 200:
            log_progress(f"Completed - Connect to the API successfully")
            data = response.json()
            df_api = pd.json_normalize(data)
            log_progress("Data was fetched successfully. Ready to be saved")

            # Saving the extraceted data into ../data directory
            save_path = os.path.join('../data', 'crypto.csv')
            # Converting the file into .CSV format
            df_api.to_csv(save_path, index=False)
            log_progress(f"Data was saved successfully at {save_path}")

        else:
            log_progress("Connect to the API unsuccessfully")
            df_api = pd.DataFrame()

    except Exception as e:
        log_progress(f"{api_status} : Encountered exception {e} while reading data from the api")
        df_api = pd.DataFrame()
    return df_api


# Extracting data through web scraping
def scraping_websites(url_link):

    article_df = pd.DataFrame()

    try:
        page = requests.get(url_link, timeout=10)
        soup = BeautifulSoup(page.content, "html.parser")
        titles = soup.find_all('h3', class_='Mb(5px)')

        # Create an empty list to store extracted data
        news_data = []

        for title in titles:
            title_element = title.find('a')  # Find hyperlink

            if title_element:  # If found
                title_text = title_element.get_text()  # Print out the article titles
                link = title_element['href']  # Print out links to articles
                news_data.append(
                    {"Title": title_text, "Link": link})  # Append the article titles along with their links to the list

        article_df = pd.DataFrame(news_data)

        # Directing data to the right folder
        output_path = os.path.join('../data', 'scraped_articles.csv')

        # Save the data frame as csv file
        article_df.to_csv(output_path, index=False, encoding='utf-8')
        log_progress(f"Data was extracted successfully and stored at ../data")

    except Exception as e:
        log_progress(f"An error occurred: {e}")

    return article_df
