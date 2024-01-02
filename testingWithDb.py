import pyodbc
from requests_html import HTMLSession
import time
import requests_cache
from urllib.parse import urlparse, urljoin
import logging

# Set up logging
#logging.basicConfig(filename='crawler.log', level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')

# Define your SQL Server connection parameters
db_connection_string = "DRIVER={SQL Server};SERVER=SQL-STAGE-2019;DATABASE=ILEngageDevdb;UID=ilengagesqldbuser;PWD=R936tzM9eVJkB64Z;Trusted_Certificate=true"


def log_to_database(url,urlCount, status_code, error_message=''):
    try:
        # Connect to SQL Server database
        conn = pyodbc.connect(db_connection_string)
        cursor = conn.cursor()

        # Insert log entry into the database
        cursor.execute('''
            INSERT INTO dbo.crawl_logs(url, status_code,error_message, timestamp,UrlCount)
            VALUES (?, ?, ?, ?,?)
        ''', (url, status_code, error_message, time.strftime('%Y-%m-%d %H:%M:%S'),urlCount))

        # Commit changes and close the connection
        conn.commit()
        conn.close()

    except Exception as e:
        url =''
        # Log exception if there's an issue with the database connection
        #logging.error(f'Error connecting to the database: {str(e)}')

try:
    url = 'https://www.hdfcfund.com/'
    domain_urls = []  # List to store URLs starting with 'https://www.hdfcfund.com/'
    total_urls = [url]
    newUrls = ['https://www.hdfcfund.com/insights/md-ceo-desk','https://www.hdfcfund.com/about-us/corporate-profile/hdfc-mf-at-a-glance','https://www.hdfcfund.com/about-us/corporate-profile/what-we-stand-for']
    url_count = 0
    url_count_failed = 0
    max_retries = 3
    x = 0

    requests_cache.install_cache('my_cache', expire_after=7200)

    for url in newUrls:
        x = x+1
        session = HTMLSession()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        try:
            response = session.get(url, headers=headers)

            if response.status_code == 200:
                response.html.render(timeout=30)
                links = response.html.find('a')

                for link in links:
                    href = link.attrs.get('href', '')
                    if href.startswith('/'):
                        absolute_url = urljoin("https://www.hdfcfund.com/", href[1:])
                        domain_urls.append(absolute_url)
                    elif href.startswith('https://files.hdfcfund.com/'):
                        domain_urls.append(href)

                domain_urls = list(set(domain_urls))
                total_urls.extend(domain_urls)
                total_urls = list(set(total_urls))
                no_urls = len(total_urls)
                domain_urls.clear()
                url_count += 1
                requests_cache.clear()
                session.delete(url)

                log_to_database(url,no_urls,status_code=200,error_message='Success')
                # Log success
                #logging.info(f'Crawling successful for URL: {url}')

            else:
                url_count_failed += 1
                # Log failure
                #logging.error(f'Failed to crawl URL: {url}. Status code: {response.status_code}')
                log_to_database(url,no_urls,status_code=101,error_message='Failed Url')

        except Exception as e:
            # Log exception
            #logging.error(f'Error while crawling URL: {url}. Exception: {str(e)}')
            # Insert log into database
            log_to_database(url, 0,status_code=500, error_message=str(e))
            continue

        finally:
            time.sleep(1)
            continue

except Exception as e:
    # Log exception
    log_to_database(url, 0,status_code=500, error_message=str(e))
