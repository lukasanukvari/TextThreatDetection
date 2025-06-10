import os
import requests
from time import sleep
from datetime import datetime, date, timedelta, timezone
from dotenv import load_dotenv

from common.utils import get_logger, write_chunk_to_json


load_dotenv()


SOURCE = 'reddit_com'
CATEGORIES = ('all', 'announcements', 'hacking', 'news', 'politics', 'startups')
CHECK_ATTRIBUTES = ('created_utc', 'author', 'title', 'id', 'permalink')


DIR_APP = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

LAST_N_DAYS = int(os.getenv('LAST_N_DAYS'))
REQUEST_RETRIES = int(os.getenv('REQUEST_RETRIES'))
REQUEST_TIMEOUT = float(os.getenv('REQUEST_TIMEOUT'))
MIN_CONTENT_LEN = float(os.getenv('MIN_CONTENT_LEN'))


def handler():
    expire_dt = date.today() - timedelta(days=LAST_N_DAYS)
    logger = get_logger(logger_name=SOURCE, logger_file_name='collectors.log')

    for c in CATEGORIES:
        healthy = True
        retry_count = 0
        after = None
        while healthy:
            sleep(3)
            if retry_count > REQUEST_RETRIES:
                logger.warning(f'{c} - Maximum retries reached. Stopping process at "after"="{after}"')
                break

            try:
                if retry_count != 0:
                    logger.warning(f'{c} - Retry {retry_count} for "after"="{after}"')

                if not after:
                    articles = requests.get(
                        f'https://reddit.com/r/{c}.json?limit=100',
                        timeout=REQUEST_TIMEOUT
                    )
                else:
                    articles = requests.get(
                        f'https://reddit.com/r/{c}.json?after={after}&limit=100',
                        timeout=REQUEST_TIMEOUT
                    )

                if articles.status_code != 200:
                    logger.error(f'{c} - Status code {articles.status_code} for "after"="{after}"')
                    retry_count += 1
                    continue

                articles = articles.json()
                if not articles or not articles['data']['children'] or not articles['data']['after']:
                    logger.info(f'{c} - Finished - data or parameter "after" missing. Previous "after"="{after}"')
                    break

                new_after = articles['data']['after']
                articles = articles['data']['children']

                chunk = []
                for a in articles:
                    a_data = a['data']

                    missing_attributes = []
                    for col in CHECK_ATTRIBUTES:
                        if col not in a_data:
                            logger.warning(
                                f'{c} - Attribute "{col}" missing '
                                f'for article: https://www.reddit.com{a_data["permalink"]}'
                            )
                            missing_attributes.append(col)
                        elif not a_data[col]:
                            logger.warning(
                                f'{c} - Attribute "{col}" empty for '
                                f'article: https://www.reddit.com{a_data["permalink"]}'
                            )

                    if 'selftext' not in a_data or not a_data['selftext']:
                        logger.warning(
                            f'{c} - Article "selftext" missing '
                            f'for article: https://www.reddit.com{a_data["permalink"]}'
                        )
                        continue

                    if 'created_utc' not in a_data or not a_data['created_utc']:
                        logger.warning(
                            f'{c} - Article "created_utc" missing '
                            f'for article: https://www.reddit.com{a_data["permalink"]}'
                        )
                        continue

                    author = None
                    if 'author' not in missing_attributes and a_data['author']:
                        author = a_data['author']

                    article_datetime = datetime.fromtimestamp(a_data['created_utc'], tz=timezone.utc)
                    article_dt = article_datetime.date()
                    if expire_dt > article_dt:
                        logger.info(
                            f'{c} - Finished at "after"={after} - '
                            'article published timestamp is older than current setting - '
                            f'[CURRENT LAST_N_DAYS SETTING: {LAST_N_DAYS}, '
                            'THUS THE EXPIRATION DATETIME: {expire_datetime}]'
                        )
                        healthy = False
                        break

                    chunk.append({
                        'id': SOURCE + '__' + (str(a_data['id']) if 'id' not in missing_attributes else '0'),
                        'source': SOURCE,
                        'author': author,
                        'article_url': a_data['permalink'],
                        'title': a_data['title'] if 'title' not in missing_attributes else None,
                        'short_description': None,
                        'created_at_utc': article_datetime.isoformat(),
                        'tags': [c],
                        'content': a_data['selftext']
                    })

                    logger.info(f'{c} - Success for article: https://www.reddit.com{a_data["permalink"]}')

                write_chunk_to_json(chunk=chunk, source=SOURCE, page_num=after, logger=logger)
                retry_count = 0
                after = new_after
            except requests.exceptions.ReadTimeout:
                logger.error(
                    f'{c} - Timeout error for "after"={after} - '
                    f'[TIMEOUT SETTING: {REQUEST_TIMEOUT} SECONDS]'
                )

                retry_count += 1
                continue
            except Exception as e:
                logger.error(f'{c} - Unexpected error for "after"={after} - {e}')

                retry_count += 1
                continue


if __name__ == '__main__':
    handler()
