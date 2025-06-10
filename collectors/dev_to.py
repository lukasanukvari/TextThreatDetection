import os
import requests
from datetime import datetime, date, timedelta, timezone
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from common.utils import get_logger, write_chunk_to_json


load_dotenv()


SOURCE = 'dev_to'
CHECK_ATTRIBUTES = ('id', 'url', 'title', 'description', 'published_at', 'tag_list', 'user')

DIR_APP = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

LAST_N_DAYS = int(os.getenv('LAST_N_DAYS'))
REQUEST_RETRIES = int(os.getenv('REQUEST_RETRIES'))
REQUEST_TIMEOUT = float(os.getenv('REQUEST_TIMEOUT'))
MIN_CONTENT_LEN = float(os.getenv('MIN_CONTENT_LEN'))


def handler():
    expire_dt = date.today() - timedelta(days=LAST_N_DAYS)
    logger = get_logger(logger_name=SOURCE, logger_file_name='collectors.log')

    healthy = True
    retry_count = 0
    pg = 1
    while healthy:
        if retry_count > REQUEST_RETRIES:
            logger.warning(f'Maximum retries reached. Stopping process at page: {pg}')
            break

        try:
            if retry_count != 0:
                logger.warning(f'Retry {retry_count} at page: {pg}')

            articles = requests.get(f'https://dev.to/api/articles?page={pg}', timeout=REQUEST_TIMEOUT)
            if articles.status_code != 200:
                logger.error(f'Status code {articles.status_code} at page: {pg}')
                retry_count += 1
                continue

            articles = articles.json()
            if not articles:
                logger.info(f'Finished - empty page: {pg}')
                break

            chunk = []
            for a in articles:
                if a['type_of'] != 'article':
                    continue

                missing_attributes = []
                for col in CHECK_ATTRIBUTES:
                    if col not in a:
                        logger.warning(f'Attribute "{col}" missing at page: {pg}')
                        missing_attributes.append(col)
                    elif not a[col]:
                        logger.warning(f'Attribute "{col}" empty at page: {pg}')

                if 'url' not in a or not a['url']:
                    logger.warning(f'Article URL missing at page: {pg}')
                    continue

                if 'published_at' not in a or not a['published_at']:
                    logger.warning(f'Article "published_at" missing for article: {a["url"]}')
                    continue

                author = None
                if 'user' not in missing_attributes and a['user']:
                    if 'name' in a['user'] and a['user']['name']:
                        author = a['user']['name']
                    else:
                        logger.warning(f'Attribute "user" -> "name" missing for article: {a["url"]}')

                article_datetime = datetime.fromisoformat(a['published_at']).astimezone(timezone.utc)
                article_dt = article_datetime.date()
                if expire_dt > article_dt:
                    logger.info(
                        f'Finished at page: {pg} - article published timestamp is older than current setting - '
                        f'[CURRENT LAST_N_DAYS SETTING: {LAST_N_DAYS}, '
                        'THUS THE EXPIRATION DATETIME: {expire_datetime}]'
                    )
                    healthy = False
                    break

                try:
                    html = requests.get(a['url']).text
                    soup = BeautifulSoup(html, 'html.parser')

                    paragraphs = soup.find('div', {'id': 'article-body'}).find_all('p')
                    content = ''
                    for p in paragraphs:
                        content += (p.get_text() + '\n')

                    if not content or len(content) <= MIN_CONTENT_LEN:
                        logger.warning(
                            f'Empty or not enough content for article: {a["url"]} - '
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
                    logger.error(f'Unexpected error for article: {a["url"]} - {e}')
                    continue

                chunk.append({
                    'id': SOURCE + '__' + (str(a['id']) if 'id' not in missing_attributes else '0'),
                    'source': SOURCE,
                    'author': author,
                    'article_url': a['url'],
                    'title': a['title'] if 'title' not in missing_attributes else None,
                    'short_description': a['description'] if 'description' not in missing_attributes else None,
                    'created_at_utc': article_datetime.isoformat(),
                    'tags': a['tag_list'] if 'tag_list' not in missing_attributes else [],
                    'content': content
                })

                logger.info(f'Success for article: {a["url"]}')

            write_chunk_to_json(chunk=chunk, source=SOURCE, page_num=pg, logger=logger)
            retry_count = 0
            pg += 1
        except requests.exceptions.ReadTimeout:
            logger.error(f'Timeout error at page: {pg} - [TIMEOUT SETTING: {REQUEST_TIMEOUT} SECONDS]')

            retry_count += 1
            continue
        except Exception as e:
            logger.error(f'Unexpected error at page: {pg} - {e}')

            retry_count += 1
            continue


if __name__ == '__main__':
    handler()
