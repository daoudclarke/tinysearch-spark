import json
from base64 import b64encode
from itertools import chain
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
from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import rand, row_number, col, create_map, lit, udf
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


def justext(html_text, stoplist, length_low=LENGTH_LOW_DEFAULT,
            length_high=LENGTH_HIGH_DEFAULT, stopwords_low=STOPWORDS_LOW_DEFAULT,
            stopwords_high=STOPWORDS_HIGH_DEFAULT, max_link_density=MAX_LINK_DENSITY_DEFAULT,
            max_heading_distance=MAX_HEADING_DISTANCE_DEFAULT, no_headings=NO_HEADINGS_DEFAULT,
            encoding=None, default_encoding=DEFAULT_ENCODING,
            enc_errors=DEFAULT_ENC_ERRORS, preprocessor=preprocessor):
    """
    Converts an HTML page into a list of classified paragraphs. Each paragraph
    is represented as instance of class ˙˙justext.paragraph.Paragraph˙˙.
    """
    dom = html_to_dom(html_text, default_encoding, encoding, enc_errors)
    print("Parsed HTML")

    try:
        title = dom.find(".//title").text
    except AttributeError:
        title = None

    preprocessed_dom = preprocessor(dom)

    paragraphs = ParagraphMaker.make_paragraphs(preprocessed_dom)
    print("Got paragraphs")

    classify_paragraphs(paragraphs, stoplist, length_low, length_high,
                        stopwords_low, stopwords_high, max_link_density, no_headings)
    revise_paragraph_classification(paragraphs, max_heading_distance)

    return paragraphs, title


class Indexer(CCSparkJob):
    output_schema = StructType([
        StructField("term_hash", LongType(), False),
        StructField("term", StringType(), False),
        StructField("uri", StringType(), False),
        StructField("title", StringType(), False),
        StructField("extract", StringType(), False),
    ])

    def process_record(self, record):
        # print("Record", record.format, record.rec_type, record.rec_headers, record.raw_stream,
        #       record.http_headers, record.content_type, record.length)

        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return
        if not self.is_html(record):
            return

        # language = record.rec_headers.get_header('WARC-Identified-Content-Language')
        # if language != 'eng':
        #     return

        uri = record.rec_headers.get_header('WARC-Target-URI')
        content = record.content_stream().read().strip()
        print("Content", uri, content[:100])

        if not content:
            return

        try:
            all_paragraphs, title = justext(content, get_stoplist('English'))
        except UnicodeDecodeError:
            print("Unable to decode unicode")
            return
        except ParserError:
            print("Unable to parse")
            return

        if title is None:
            print("Missing title")
            return

        text = '\n'.join([p.text for p in all_paragraphs
                          if not p.is_boilerplate])[:NUM_CHARS_TO_ANALYSE]
        print("Paragraphs", text)

        if len(text) < NUM_EXTRACT_CHARS:
            return

        language = detect(text)
        print("Got language", language)
        if language != 'en':
            return

        terms = set()
        extract_tokens = nlp.tokenizer(text)
        extract = extract_terms(extract_tokens, terms, NUM_EXTRACT_CHARS)

        if not extract:
            return

        for term in terms:
            key_hash = mmh3.hash(term, signed=False)
            key = key_hash % NUM_PAGES
            yield key, term, uri, title, extract

    def run_job(self, sc, sqlc):
        input_data = sc.textFile(self.args.input,
                                 minPartitions=self.args.num_input_partitions)

        output = self.process_data(input_data, sqlc)

        output.write.format('json').save('results')

        # sqlc.createDataFrame(output, schema=self.output_schema) \
        #     .coalesce(self.args.num_output_partitions) \
        #     .write \
        #     .format(self.args.output_format) \
        #     .option("compression", self.args.output_compression) \
        #     .options(**self.get_output_options()) \
        #     .saveAsTable(self.args.output)

        self.log_aggregators(sc)

    def process_data(self, input_data, sqlc) -> DataFrame:
        rdd = input_data.mapPartitionsWithIndex(self.process_warcs)
        df = sqlc.createDataFrame(rdd, schema=self.output_schema)
        # domain_mapping = create_map([lit(x) for x in chain(*DOMAIN_RATINGS.items())])
        # df = df.withColumn('domain_rating', domain_mapping.getItem(col("key")))
        df = df.withColumn('domain_rating', get_domain_rating('uri'))
        window = Window.partitionBy(df['term_hash']).orderBy(col('domain_rating').desc())
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

    return pd.DataFrame([{'term_hash': term_hash, 'data': encoded}])


def compress(results):
    items = results[['term', 'uri', 'domain_rating', 'title', 'extract']].to_dict('records')
    serialised_data = json.dumps(items)
    return serialised_data
    # compressed_data = zstandard.compress(serialised_data.encode('utf8'))
    # encoded = b64encode(compressed_data)
    # return encoded


if __name__ == '__main__':
    job = Indexer()
    job.run()
