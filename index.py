import argparse
import json
from base64 import b64encode
from collections import Iterator
from itertools import chain
from typing import Iterable
from urllib.parse import urlparse

import mmh3
import pandas as pd
import spacy as spacy
import zstandard
from justext import get_stoplist
from justext.core import LENGTH_LOW_DEFAULT, LENGTH_HIGH_DEFAULT, STOPWORDS_LOW_DEFAULT, \
    STOPWORDS_HIGH_DEFAULT, MAX_LINK_DENSITY_DEFAULT, NO_HEADINGS_DEFAULT, \
    MAX_HEADING_DISTANCE_DEFAULT, DEFAULT_ENCODING, DEFAULT_ENC_ERRORS, preprocessor, html_to_dom, \
    ParagraphMaker, classify_paragraphs, revise_paragraph_classification
from langdetect import detect
from lxml.etree import ParserError
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import rand, row_number, col, create_map, lit, udf
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
from spacy.tokens import Token, Span
from zstandard import ZstdCompressor

from domains import TOP_DOMAINS_PATH
from sparkcc import CCSparkJob

NUM_CHARS_TO_ANALYSE = 1000
NUM_TITLE_CHARS = 65
NUM_EXTRACT_CHARS = 155
NUM_PAGES = 1024
MAX_RESULTS_PER_HASH = 200
PAGE_SIZE = 4096


nlp = spacy.load("en_core_web_sm", disable=['lemmatizer', 'ner'])


index_schema = StructType([
    StructField("term_hash", LongType(), False),
    StructField("data", StringType(), False),
    StructField("top", StringType(), False),
])


DOMAIN_RATINGS = json.load(open(TOP_DOMAINS_PATH))


@udf(returnType=FloatType())
def get_domain_rating(url):
    domain = urlparse(url).netloc
    return DOMAIN_RATINGS.get(domain, 0.0)


def is_valid_token(token: Token):
    return not token.is_punct and not token.is_space and not token.is_stop


def extract_terms(span: Span, terms: set[str], max_chars: int) -> str:
    extract = None
    for token in span:
        text = span[:token.idx + 1].text
        if len(text) > max_chars - 1:
            return extract + '…' if extract is not None else None
        extract = text
        if is_valid_token(token):
            terms.add(str(token).lower())
    return extract


@udf(returnType=FloatType())
def get_relevance(term, title, extract):
    title_location = title.lower().find(term)
    if title_location >= 0:
        before_count = title_location
        after_count = len(title) - len(term)
        score = 1.0
    else:
        extract_location = extract.lower().find(term)
        assert extract_location >= 0
        before_count = extract_location
        after_count = 0
        score = 0.3
    score *= 0.98 ** before_count * 0.99 ** after_count
    return score


record_schema = StructType([
    StructField("term_hash", LongType(), False),
    StructField("term", StringType(), False),
    StructField("uri", StringType(), False),
    StructField("title", StringType(), False),
    StructField("extract", StringType(), False),
])


def get_terms(record_batches: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for records in record_batches:
        term_rows = []
        for row in records.itertuples():
            terms = set()
            extract_tokens = nlp.tokenizer(row.extract)
            extract = extract_terms(extract_tokens, terms, NUM_EXTRACT_CHARS)

            title_terms = nlp.tokenizer(row.title)
            title_tidied = extract_terms(title_terms, terms, NUM_TITLE_CHARS)

            if not extract:
                return

            for term in terms:
                key_hash = mmh3.hash(term, signed=False)
                key = key_hash % NUM_PAGES
                term_rows.append({
                    'term_hash': key,
                    'term': term,
                    'uri': row.uri,
                    'title': title_tidied,
                    'extract': extract,
                })
        yield pd.DataFrame(term_rows)


def create_index(input_df: DataFrame) -> DataFrame:
    df = input_df.mapInPandas(get_terms, record_schema)
    df = df.withColumn('domain_rating', get_domain_rating('uri'))
    df = df.withColumn('relevance', get_relevance('term', 'title', 'extract'))
    df = df.withColumn('score', col('domain_rating') * col('relevance'))
    window = Window.partitionBy(df['term_hash']).orderBy(col('score').desc())
    ranked = df.select('*', row_number().over(window).alias('rank')) \
        .filter(col('rank') <= MAX_RESULTS_PER_HASH)
    output = ranked.groupby('term_hash').applyInPandas(compress_group, schema=index_schema)
    return output


def compress_group(results: pd.DataFrame) -> pd.DataFrame:
    term_hashes = results['term_hash'].unique()
    assert len(term_hashes) == 1
    term_hash = term_hashes[0]

    lower = 0
    upper = len(results)
    num_to_select = len(results) // 2
    while upper - lower > 1:
        encoded = compress(results.iloc[:num_to_select])
        size = len(encoded)
        if size > PAGE_SIZE:
            upper = num_to_select
            num_to_select = (num_to_select + lower) // 2
        else:
            lower = num_to_select
            num_to_select = (num_to_select + upper) // 2
        print(f"Lower {lower}, upper {upper}")
    encoded = compress(results.iloc[:lower])

    top_result = json.dumps(results.iloc[0].to_dict())
    return pd.DataFrame([{'term_hash': term_hash, 'data': encoded, 'top': top_result}])


def compress(results):
    selected_columns = results[['term', 'uri', 'title', 'extract']]
    item_dicts = selected_columns.to_dict('records')
    items = [list(item.values()) for item in item_dicts]
    serialised_data = json.dumps(items)
    compressed_data = zstandard.compress(serialised_data.encode('utf8'))
    encoded = b64encode(compressed_data)
    return encoded


def run(input_path, output_path):
    conf = SparkConf()

    sc = SparkContext(
        appName=__name__,
        conf=conf)
    sqlc = SQLContext(sparkContext=sc)
    extracts_df = sqlc.read.format('json').option('compress', 'gzip').load(input_path)
    print("Got extracts", extracts_df.take(10))
    index = create_index(extracts_df)
    index.write.format('json').save(output_path)


def parse_arguments():
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("input", help="Path to file listing input paths")
    arg_parser.add_argument("output",
                            help="Name of output table (saved in spark.sql.warehouse.dir)")

    return arg_parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    run(args.input, args.output)
