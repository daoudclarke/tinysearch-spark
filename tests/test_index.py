from pathlib import Path
from random import Random

import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession

from index import Indexer, compress_group, PAGE_SIZE

DATA_PATH = Path(__file__).parent / 'data' / 'data.warc.gz'
random = Random(1)


def test_indexer(spark_context: SparkContext):
    indexer = Indexer()
    indexer.init_accumulators(spark_context)

    sql_context = SparkSession.builder.getOrCreate()
    data = spark_context.parallelize([f'file:{DATA_PATH}'])

    processed = indexer.create_index(data, sql_context).collect()

    assert len(processed) > 0


def shuffle(s):
    l = list(s)
    random.shuffle(l)
    return ''.join(l)


def make_test_data(num_items):
    data = {
        'term_hash': [37] * num_items,
        'term': ['boring'] * num_items,
        'uri': [f'https://{shuffle("somethingwebsiteidontknow")}.com' for _ in range(num_items)],
        'title': [shuffle('Some Really Long and Boring Title About St.') for _ in range(num_items)],
        'extract': [shuffle('Instructors of “Introduction to programming” courses know that '
                            'students are willing to blame the failures of their programs on '
                            'anything. Sorting routine discards half of the data? '
                            '“That might be a Windows virus!” Binary search always fails?')
                    for _ in range(num_items)],
    }
    data_frame = pd.DataFrame(data)
    return data_frame


def test_compress_group_too_big():
    num_items = 100
    data_frame = make_test_data(num_items)

    compressed = compress_group(data_frame)
    data = compressed['data'].iloc[0]
    print("Compressed", data)
    assert 0 < len(data) < PAGE_SIZE


def test_compress_group_large_item():
    num_items = 5
    data = {
        'term_hash': [37] * num_items,
        'term': ['boring'] * num_items,
        'uri': [f'https://{shuffle("somethingwebsiteidontknow")}.com' for _ in range(num_items)],
        'title': [shuffle('Some Really Long and Boring Title About St.') for _ in range(num_items)],
        'extract': [shuffle('Instructors of “Introduction to programming” courses know that '
                            'students are willing to blame the failures of their programs on '
                            'anything. Sorting routine discards half of the data? '
                            '“That might be a Windows virus!” Binary search always fails?'*5000)
                    for _ in range(num_items)],
    }
    data_frame = pd.DataFrame(data)

    compressed = compress_group(data_frame)
    data = compressed['data'].iloc[0]
    print("Compressed", data)
    assert 0 < len(data) < PAGE_SIZE
