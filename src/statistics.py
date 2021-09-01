from abc import abstractmethod
import os
from delta.tables import *
from delta import *
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.utils import AnalysisException

from src.utils import read_stream, write_stream


class Statistics:
    """
    Base class for different statistics to be implemented.

    :param base_dir: Location to base directory. 
    (optional, default: current dir)
    :param input_format: Input format of the source like 
    socket,parquet etc. (optional, default: json)
    :param input_path: Input location of the source files to 
    stream. (optional, default: source)
    :param schema: Schema of the input data to be streamed. 
    (optional, default: None)
    """

    def __init__(
        self,
        base_dir=os.getcwd(),
        input_format="json",
        input_path="source",
        schema=None,
    ):
        spark_builder = (
            SparkSession.builder.master("local[6]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.streaming.concurrentJobs", "2")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        self.spark = configure_spark_with_delta_pip(
            spark_builder
        ).getOrCreate()

        self.base_dir = base_dir
        self.input_format = input_format
        self.input_path = f"{base_dir}/{input_path}"
        self.schema = schema

    @abstractmethod
    def create_upsert_func(self, path):
        """Function that can be used in foreachBatch for writes."""
        pass

    @abstractmethod
    def run(self):
        """Implementation of aggregations."""
        pass


class PageUserCount(Statistics):
    def create_upsert_func(self, batch_df, batch_id):
        """
        Function run foreachBatch writeStream.
        It calculates the daily_count for page views and user-page views/
        Updates/Inserts the count depending on if
        key i.e pageId or userId,pageId, already exists
        """
        batch_df.persist()
        page_daily_counts = (
            batch_df.groupBy("pageId", "date")
            .agg(F.count("pageId").alias("count"))
            .selectExpr(
                "pageId as key",
                "date",
                "count as daily_count"
            )
        )

        user_page_daily_counts = (
            batch_df.groupBy("userId", "pageId", "date")
            .agg(F.count("pageId").alias("count"))
            .selectExpr(
                "concat_ws(',', userId, pageId) as key",
                "date",
                "count as daily_count"
            )
        )

        final_df = page_daily_counts.unionByName(user_page_daily_counts)
        if DeltaTable.isDeltaTable(self.spark, self.path):
            existing_counts = self.spark.read.format("delta").load(self.path)
            existing_counts.show()
            final_df = (
                final_df
                .withColumnRenamed("daily_count", "daily_count_updates")
                .join(existing_counts, ["key", "date"], "left")
                .selectExpr(
                    "key",
                    "date",
                    "daily_count_updates+coalesce(daily_count,0) as daily_count",
                )
            )
            final_df.show()

            target_df = DeltaTable.forPath(self.spark, self.path)
            (
                target_df.alias("target")
                .merge(
                    source=final_df.alias("updates"),
                    condition="target.key = updates.key AND target.date = updates.date",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            final_df.write.format("delta").mode("append").save(self.path)
        batch_df.unpersist()

    @property
    def run(self):
        self.path = f"{self.base_dir}/page_count_aggregations"
        checkpoint_loc = f"{self.base_dir}/checkpoint"

        if not os.path.exists(self.path):
            os.makedirs(self.path)
        if not os.path.exists(checkpoint_loc):
            os.makedirs(checkpoint_loc)
        result_schema = "key string, date date, last_7day_count int"

        stream_input_df = read_stream(
            self.schema,
            self.spark,
            self.input_format,
            self.input_path,
            read_options={"multiline": "true"},
        )
        stream_input_df = stream_input_df.withColumn(
            "date", F.to_date(F.to_timestamp(F.col("timestamp") / 1000))
        )

        ingest_records_stream = write_stream(
            stream_input_df,
            func=self.create_upsert_func,
            checkpoint=checkpoint_loc,
            output_sink="delta",
        )
        ingest_records_stream.awaitTermination()
        try:
            table_exists = DeltaTable.isDeltaTable(self.spark, self.path)
            # Dirty HACK for when the delta table isnt there yet.
            if table_exists:
                # TODO: Fix this to only print updates
                count_updates = read_stream(
                    result_schema, self.spark, "delta", self.path
                )
            else:
                count_updates = (
                    self.spark.read.format("delta")
                    .load(self.path)
                )
            count_updates = (
                count_updates
                .filter("date >= current_date() - interval 6 days")
                .groupBy("key")
                .agg(F.sum("daily_count").alias("last_7day_count"))
                .selectExpr("key", "current_date() as date", "last_7day_count")
            )
            if table_exists:
                console_print_stream = write_stream(count_updates)
                console_print_stream.awaitTermination()
            else:
                count_updates.show()
        except AnalysisException as e:
            print(f"No Data found in {self.input_path}")
