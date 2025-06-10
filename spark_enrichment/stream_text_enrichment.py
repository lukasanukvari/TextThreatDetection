import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from delta.tables import DeltaTable


from common.utils import get_logger


# Setup paths
DIR_APP = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DIR_MAPPING = os.path.join(DIR_APP, 'config', 'threat_mapping.json')
DIR_DELTA_INPUT = os.path.join(DIR_APP, 'delta_lake', 'text_table')
DIR_DELTA_OUTPUT = os.path.join(DIR_APP, 'delta_lake', 'enriched_table')
DIR_CHECKPOINT = os.path.join(DIR_APP, 'checkpoints', 'enrichment')


logger = get_logger(logger_name='stream_enrichment', logger_file_name='stream_enrichment.log')


with open(DIR_MAPPING, 'r') as f:
    threat_map = json.load(f)

flat_patterns = []
for category, entries in threat_map.items():
    for entry in entries:
        flat_patterns.append((category, entry['pattern'], entry['severity']))


broadcast_map = flat_patterns


spark = SparkSession.builder.appName('threat_stream_enrichment') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')


def match_threats(text):
    matches = []
    for cat, keyword, severity in broadcast_map:
        if keyword.lower() in text.lower():
            matches.append({
                'category': cat,
                'keyword': keyword,
                'severity': severity
            })
    return matches


match_threats_udf = udf(match_threats, ArrayType(
    StructType([
        StructField('category', StringType()),
        StructField('keyword', StringType()),
        StructField('severity', StringType())
    ])
))


df_stream = spark.readStream.format('delta').load(DIR_DELTA_INPUT)

df_enriched = df_stream.withColumn('threat_matches', match_threats_udf(col('content'))) \
   .withColumn('match', explode('threat_matches')) \
   .select(
       col('id'),
       col('content'),
       col('created_at_utc'),
       col('match.category'),
       col('match.keyword'),
       col('match.severity'),
   )


def upsert_to_delta(batch_df, batch_id):
    try:
        logger.info(f'Processing batch {batch_id} with {batch_df.count()} rows.')
        if DeltaTable.isDeltaTable(spark, DIR_DELTA_OUTPUT):
            delta_table = DeltaTable.forPath(spark, DIR_DELTA_OUTPUT)
            delta_table.alias('target').merge(
                batch_df.alias('source'),
                'target.id = source.id AND target.keyword = source.keyword'
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            batch_df.write.format('delta').mode('overwrite').save(DIR_DELTA_OUTPUT)
        logger.info(f'Batch {batch_id} processed.')
    except Exception as e:
        logger.error(f'Unexpected error for batch {batch_id}: {e}')


query = df_enriched.writeStream.foreachBatch(upsert_to_delta).option('checkpointLocation', DIR_CHECKPOINT).start()

query.awaitTermination()
