import re

import mmh3
import spacy as spacy
from justext import get_stoplist
from justext.core import LENGTH_LOW_DEFAULT, LENGTH_HIGH_DEFAULT, STOPWORDS_LOW_DEFAULT, \
    STOPWORDS_HIGH_DEFAULT, MAX_LINK_DENSITY_DEFAULT, NO_HEADINGS_DEFAULT, \
    MAX_HEADING_DISTANCE_DEFAULT, DEFAULT_ENCODING, DEFAULT_ENC_ERRORS, preprocessor, html_to_dom, \
    ParagraphMaker, classify_paragraphs, revise_paragraph_classification
from langdetect import detect
from lxml.etree import ParserError
from pyspark.sql.types import StructType, StructField, StringType, LongType
from spacy.pipeline import Sentencizer
from spacy.tokens import Doc, Token, Span

from sparkcc import CCSparkJob


NUM_CHARS_TO_ANALYSE = 1000
NUM_TITLE_CHARS = 65
NUM_EXTRACT_CHARS = 155
NUM_PAGES = 1024


nlp = spacy.load("en_core_web_sm", disable=['lemmatizer', 'ner'])
sentencizer = Sentencizer(Sentencizer.default_punct_chars + ['\n'])


# def tokenizer(sentence):
#     parsed = nlp.tokenizer(sentence)
#     return [str(token).lower() for token in parsed
#             if not token.is_punct
#             and not token.is_space]


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
    dom = preprocessor(dom)

    paragraphs = ParagraphMaker.make_paragraphs(dom)

    classify_paragraphs(paragraphs, stoplist, length_low, length_high,
                        stopwords_low, stopwords_high, max_link_density, no_headings)
    revise_paragraph_classification(paragraphs, max_heading_distance)

    try:
        title = dom.find(".//title").text
    except AttributeError:
        title = None
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
        print("Content", content[:100])

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

        # title_tokens = nlp.tokenizer(paragraphs[0])

        terms = set()
        # title = extract_terms(title_tokens, terms, NUM_TITLE_CHARS)
        #
        # if not title:
        #     return

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

        # output = input_data.mapPartitionsWithIndex(self.process_warcs) \
        #     .reduceByKey(self.reduce_by_key_func)

        output = input_data.mapPartitionsWithIndex(self.process_warcs)

        sqlc.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression) \
            .options(**self.get_output_options()) \
            .saveAsTable(self.args.output)

        self.log_aggregators(sc)


if __name__ == '__main__':
    job = Indexer()
    job.run()
