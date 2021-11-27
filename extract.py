"""
Extract content from HTML files and store it as compressed JSON
"""

import json
import os
from pathlib import Path
from urllib.parse import urlparse

import spacy as spacy
from justext import get_stoplist
from justext.core import LENGTH_LOW_DEFAULT, LENGTH_HIGH_DEFAULT, STOPWORDS_LOW_DEFAULT, \
    STOPWORDS_HIGH_DEFAULT, MAX_LINK_DENSITY_DEFAULT, NO_HEADINGS_DEFAULT, \
    MAX_HEADING_DISTANCE_DEFAULT, DEFAULT_ENCODING, DEFAULT_ENC_ERRORS, preprocessor, html_to_dom, \
    ParagraphMaker, classify_paragraphs, revise_paragraph_classification
from langdetect import detect
from lxml.etree import ParserError
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCSparkJob

DATA_DIR = Path(os.environ['HOME']) / 'data' / 'tinysearch'
TOP_DOMAINS_PATH = DATA_DIR / 'hn-top-domains-filtered.json'

MAX_URI_LENGTH = 150
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


def get_domain_rating(url):
    domain = urlparse(url).netloc
    return DOMAIN_RATINGS.get(domain)


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


class Extractor(CCSparkJob):
    output_schema = StructType([
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

        uri = record.rec_headers.get_header('WARC-Target-URI')
        if len(uri) > MAX_URI_LENGTH:
            print("URI too long", len(uri))
            return

        rating = get_domain_rating(uri)
        print("Rating", rating)
        if rating is None:
            return

        content = record.content_stream().read().strip()
        print("Content", uri, content[:100])

        if not content:
            return

        try:
            all_paragraphs, full_title = justext(content, get_stoplist('English'))
        except UnicodeDecodeError:
            print("Unable to decode unicode")
            return
        except ParserError:
            print("Unable to parse")
            return

        if full_title is None:
            print("Missing title")
            return

        title = full_title[:NUM_TITLE_CHARS] + '…' \
            if len(full_title) > NUM_TITLE_CHARS else full_title

        text = '\n'.join([p.text for p in all_paragraphs
                          if not p.is_boilerplate])[:NUM_CHARS_TO_ANALYSE]
        print("Paragraphs", text)

        if len(text) < NUM_EXTRACT_CHARS:
            return

        language = detect(text)
        print("Got language", language)
        if language != 'en':
            return

        extract = text[:NUM_EXTRACT_CHARS]
        yield uri, title, extract

    def run_job(self, sc, sqlc):
        input_data = sc.textFile(self.args.input,
                                 minPartitions=self.args.num_input_partitions)

        output = self.process_data(input_data, sqlc)
        output.write.option('compression', 'gzip').format('json').save(self.args.output)

        self.log_aggregators(sc)

    def process_data(self, input_data, sqlc) -> DataFrame:
        rdd = input_data.mapPartitionsWithIndex(self.process_warcs)
        return sqlc.createDataFrame(rdd, schema=self.output_schema)


if __name__ == '__main__':
    job = Extractor()
    job.run()
