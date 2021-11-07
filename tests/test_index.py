from pathlib import Path

from pyspark import SparkContext
from pyspark.sql import SparkSession

from index import Indexer

DATA_PATH = Path(__file__).parent / 'data' / 'data.warc.gz'


def test_indexer(spark_context: SparkContext):
    indexer = Indexer()
    indexer.init_accumulators(spark_context)

    sql_context = SparkSession.builder.getOrCreate()
    data = spark_context.parallelize([f'file:{DATA_PATH}'])

    processed = indexer.process_data(data, sql_context).collect()

    assert len(processed) > 0
