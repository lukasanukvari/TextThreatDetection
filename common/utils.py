import os
import json
import logging
from typing import List


DIR_APP = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DIR_LOGS = os.path.join(DIR_APP, 'logs')
DIR_DATA_DST = os.path.join(DIR_APP, 'data', 'raw')


def get_logger(logger_name: str, logger_file_name: str = None):
    os.makedirs(DIR_LOGS, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        filename=os.path.join(DIR_LOGS, logger_file_name),
        filemode='a'
    )

    return logging.getLogger(logger_name)


def write_chunk_to_json(
    chunk: List[dict],
    source: str,
    page_num: str | int = None,
    logger: logging.Logger = None
):
    if not chunk:
        if logger:
            logger.warning(f'Empty chunk for {source} - {page_num}')
        return

    os.makedirs(DIR_DATA_DST, exist_ok=True)

    fname = os.path.join(DIR_DATA_DST, (f'{source}_pg_{page_num}.json' if page_num else f'{source}.json'))
    with open(fname, 'w', encoding='utf-8') as f:
        json.dump(chunk, f, ensure_ascii=False, indent=2)

    if logger:
        logger.info(f'Wrote {len(chunk)} articles to "{fname}"')

#
#
#
# from PIL import Image
# from io import BytesIO
# import pytesseract
# import requests
#
# # Load the uploaded image
# image_path = "https://media2.dev.to/dynamic/image/width=1000,height=420,fit=cover,gravity=auto,format=auto/https%3A%2F%2Fdev-to-uploads.s3.amazonaws.com%2Fi%2Ft3ocdzsevtagnvobno5r.png"
# image = BytesIO(requests.get(image_path).content)
# image = Image.open(image)
#
# # Use pytesseract to extract text
# extracted_text = pytesseract.image_to_string(image)
#
# print(extracted_text)