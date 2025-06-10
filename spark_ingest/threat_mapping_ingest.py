import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from common.utils import get_logger


load_dotenv()


JAVA_HOME = os.getenv('JAVA_HOME')
DELTA_PATH = os.getenv('DELTA_OUTPUT_PATH')

DIR_APP = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
THREAT_MAPPING_PATH = os.path.join(DIR_APP, 'config', 'threat_mapping.json')
DELTA_OUTPUT_PATH = os.path.join(DIR_APP, 'delta_lake', 'threat_mapping')


spark = SparkSession.builder \
    .appName('Threat Mapping Ingestion') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

logger = get_logger(logger_name='threat_mapping_ingest', logger_file_name='ingestion.log')


with open(THREAT_MAPPING_PATH, 'r') as f:
    threat_data = json.load(f)

try:
    records = []
    for category, patterns in threat_data.items():
        for p in patterns:
            records.append({
                'category': category,
                'pattern': p['pattern'],
                'severity': p['severity']
            })

    df = spark.createDataFrame(records)
    df.write.format('delta').mode('overwrite').save(DELTA_OUTPUT_PATH)

    logger.info('Threat mapping ingestion complete')
except Exception as e:
    logger.error(f'Unexpected error: {e}')
finally:
    spark.stop()
