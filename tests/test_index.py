from pathlib import Path

from pyspark import SparkContext, SQLContext

from index import Indexer


DATA_PATH = Path(__file__).parent / 'data' / 'data.warc.gz'


def test_indexer(spark_context: SparkContext):
    indexer = Indexer()
    indexer.init_accumulators(spark_context)

    sql_context = SQLContext(spark_context)
    data = spark_context.parallelize([f'file:{DATA_PATH}'])
    # data = sql_context.createDataFrame([{'file': 'test_data'}])

    processed = indexer.process_data(data, sql_context).collect()

    assert processed == 2
