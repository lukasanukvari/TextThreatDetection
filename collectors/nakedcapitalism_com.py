import os
import requests
from datetime import datetime, date, timedelta, timezone
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from common.utils import get_logger, write_chunk_to_json


load_dotenv()


SOURCE = 'nakedcapitalism_com'
CHECK_ATTRIBUTES = ()

DIR_APP = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

LAST_N_DAYS = int(os.getenv('LAST_N_DAYS'))
REQUEST_RETRIES = int(os.getenv('REQUEST_RETRIES'))
REQUEST_TIMEOUT = float(os.getenv('REQUEST_TIMEOUT'))
MIN_CONTENT_LEN = float(os.getenv('MIN_CONTENT_LEN'))


def handler():
    expire_datetime = date.today() - timedelta(days=LAST_N_DAYS)
    logger = get_logger(logger_name=SOURCE, logger_file_name='collectors.log')

    retry_count = 0
    for_date = date.today() - timedelta(days=5)
    while expire_datetime <= for_date:
        dt_path = for_date.strftime('%Y/%m/%d')
        dt_i = for_date.strftime('%Y_%m_%d')
        if retry_count > REQUEST_RETRIES:
            logger.warning(
                'Maximum retries reached. '
                f'Stopping process at page: https://www.nakedcapitalism.com/{dt_path}'
            )
            break

        try:
            if retry_count != 0:
                logger.warning(f'Retry {retry_count} at page: https://www.nakedcapitalism.com/{dt_path}')

            articles = requests.get(f'https://www.nakedcapitalism.com/{dt_path}', timeout=REQUEST_TIMEOUT)
            if articles.status_code == 404:
                logger.info(f'Empty page: https://www.nakedcapitalism.com/{dt_path}')

                retry_count = 0
                dt_path += 1
                continue
            elif articles.status_code != 200:
                logger.error(
                    f'Status code {articles.status_code} '
                    f'at page: https://www.nakedcapitalism.com/{dt_path}'
                )

                retry_count += 1
                continue

            articles = BeautifulSoup(articles.text, 'html.parser').find('div', {'id': 'content'})
            articles = articles.find_all('article')
            if not articles:
                logger.info(f'Empty page: https://www.nakedcapitalism.com/{dt_path}')

                retry_count = 0
                dt_path += 1
                continue

            chunk = []
            for a in articles:
                title = a.find('h2', {'class': 'entry-title'}).find('a').get_text()
                url = a.find('h2', {'class': 'entry-title'}).find('a').get('href')
                short_descr = a.find('div', {'class': 'entry-summary'}).find('p').get_text()

                if not url:
                    logger.warning(f'Article URL missing at page: https://www.nakedcapitalism.com/{dt_path}')
                    continue

                try:
                    html = requests.get(url).text
                    soup = BeautifulSoup(html, 'html.parser')

                    author = soup.find('span', {'class': 'author vcard'}).find('a').get_text()
                    tags = soup.find('footer', {'class': 'entry-meta'}).find_all('a', {'rel': 'category tag'})
                    tags = [t.get_text() for t in tags] if tags else []

                    created_at = soup.find('time', {'class': 'entry-date updated'}).get('datetime')
                    created_at = datetime.fromisoformat(created_at).astimezone(timezone.utc).isoformat()

                    paragraphs = soup.find('div', {'class': 'entry-content'}).find('div', {'class': 'pf-content'})
                    content = ''
                    for p in paragraphs:
                        content += (p.get_text() + '\n')

                    if not content or len(content) <= MIN_CONTENT_LEN:
                        logger.warning(
                            f'Empty or not enough content for article: {url} - '
                            f'[CURRENT CONTENT MINIMUM LENGTH SETTING: {MIN_CONTENT_LEN} CHARACTERS]'
                        )
                        continue
                except requests.exceptions.ReadTimeout:
                    logger.error(
                        f'Timeout error for article: {a["url"]} - '
                        f'[TIMEOUT SETTING: {REQUEST_TIMEOUT} SECONDS]'
                    )
                    continue
                except Exception as e:
                    logger.error(f'Unexpected error for article: {url} - {e}')
                    continue

                chunk.append({
                    'id': SOURCE + '__' + a.get('id').replace('post-', ''),
                    'source': SOURCE,
                    'author': author,
                    'article_url': url,
                    'title': title,
                    'short_description': short_descr,
                    'created_at_utc': created_at,
                    'tags': tags,
                    'content': content
                })

                logger.info(f'Success for article: {url}')

            write_chunk_to_json(chunk=chunk, source=SOURCE, page_num=dt_i, logger=logger)
            retry_count = 0
            for_date -= timedelta(days=1)
        except requests.exceptions.ReadTimeout:
            logger.error(
                f'Timeout error at page: https://www.nakedcapitalism.com/{dt_path} - '
                f'[TIMEOUT SETTING: {REQUEST_TIMEOUT} SECONDS]'
            )

            retry_count += 1
            continue
        except Exception as e:
            logger.error(f'Unexpected error at page: https://www.nakedcapitalism.com/{dt_path} - {e}')

            retry_count += 1
            continue


if __name__ == '__main__':
    handler()
