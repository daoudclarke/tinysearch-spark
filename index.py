import re

from pyspark.sql.types import StructType, StructField, StringType

from sparkcc import CCSparkJob


class Indexer(CCSparkJob):
    word_pattern = re.compile(r'\w+', re.UNICODE)

    output_schema = StringType()

    def process_record(self, record):
        if not self.is_wet_text_record(record):
            return
        data = record.content_stream().read().decode('utf-8')
        yield data[:1000]

        # words = map(lambda w: w.lower(),
        #             Indexer.word_pattern.findall(data))
        # for word in words:
        #     yield word, data[:100]

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
