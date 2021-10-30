import re

import mmh3
from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCSparkJob


NUM_CHARS = 1000
NUM_PAGES = 1024


class Indexer(CCSparkJob):
    word_pattern = re.compile(r'\w+', re.UNICODE)

    output_schema = StructType([
        StructField("term_hash", LongType(), True),
        StructField("uri", StringType(), True),
        StructField("extract", StringType(), True),
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

        words = map(lambda w: w.lower(),
                    Indexer.word_pattern.findall(extract))
        for word in words:
            key_hash = mmh3.hash(word, signed=False)
            key = key_hash % NUM_PAGES

            yield key, uri, extract

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
