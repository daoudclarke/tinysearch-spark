import re

import mmh3
import spacy as spacy
from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCSparkJob


NUM_CHARS = 1000
NUM_PAGES = 1024


nlp = spacy.load("en_core_web_sm")


# def tokenizer(sentence):
#     parsed = nlp.tokenizer(sentence)
#     return [str(token).lower() for token in parsed
#             if not token.is_punct
#             and not token.is_space]


class Indexer(CCSparkJob):
    output_schema = StructType([
        StructField("term_hash", LongType(), False),
        StructField("term", StringType(), False),
        StructField("uri", StringType(), False),
        StructField("extract", StringType(), False),
    ])

    def process_record(self, record):
        if not self.is_wet_text_record(record):
            return

        # print("Process", record.format, record.rec_type, record.rec_headers, record.raw_stream,
        #       record.http_headers, record.content_type, record.length)

        language = record.rec_headers.get_header('WARC-Identified-Content-Language')
        if language != 'eng':
            return

        uri = record.rec_headers.get_header('WARC-Target-URI')
        extract = record.content_stream().read().decode('utf-8')[:NUM_CHARS]

        tokens = nlp.tokenizer(extract)

        for token in tokens[:-1]:
            if token.is_punct or token.is_space or token.is_stop:
                continue

            term = str(token).lower()
            key_hash = mmh3.hash(term, signed=False)
            key = key_hash % NUM_PAGES
            yield key, term, uri, extract

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
