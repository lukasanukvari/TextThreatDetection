import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from delta.tables import DeltaTable

from common.utils import get_logger


load_dotenv()


JAVA_HOME = os.getenv('JAVA_HOME')
DELTA_PATH = os.getenv('DELTA_OUTPUT_PATH')

DIR_APP = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DIR_DATA_SRC = os.path.join(DIR_APP, 'data', 'raw')
DELTA_OUTPUT_PATH = os.path.join(DIR_APP, 'delta_lake', 'text_table')


logger = get_logger(logger_name='text_ingest', logger_file_name='ingestion.log')

spark = SparkSession.builder \
    .appName('text_data_ingestion') \
    .config('spark.jars.packages', 'io.delta:delta-core_2.12:2.4.0') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()


try:
    df = spark.read.option('multiline', 'true').json(f'{DIR_DATA_SRC}/*.json')

    # One more layer for data validation
    df_cleaned = df.dropna(subset=['id', 'content'])
    df_cleaned = df_cleaned.dropDuplicates(['id'])
    df_cleaned = df_cleaned.withColumn('created_at_utc', to_timestamp('created_at_utc'))

    row_count = df_cleaned.count()
    logger.info(f'{row_count} rows ready for ingestion into Delta Lake.')

    if DeltaTable.isDeltaTable(spark, DELTA_OUTPUT_PATH):
        logger.info('Merging (upserting) data into Delta Lake.')

        delta_table = DeltaTable.forPath(spark, DELTA_OUTPUT_PATH)
        delta_table.alias('target').merge(
            df_cleaned.alias('source'),
            'target.id = source.id'
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        logger.info('Creating/overwriting Delta Lake.')
        df_cleaned.write.format('delta').mode('overwrite').partitionBy('created_at_utc').save(DELTA_OUTPUT_PATH)

    logger.info('Ingestion to Delta Lake complete.')
except Exception as e:
    logger.error(f'Unexpected error: {e}')
finally:
    spark.stop()
