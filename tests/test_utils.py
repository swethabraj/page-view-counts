import os
import shutil
from pandas.testing import assert_frame_equal

from src.utils import read_stream, write_stream

curr_dir = os.getcwd()
schema = "pageId string, userId string, timestamp long"


def test_read_stream_pass(spark):
    read_stream_df = read_stream(
        schema,
        spark,
        input_format="json",
        input_path=f"{curr_dir}/tests/data/raw",
        read_options={"mutliline": "true"},
    )
    assert read_stream_df.isStreaming


def test_write_stream(spark):
    checkpoint_loc = f"{curr_dir}/tests/checkpoint"

    read_stream_df = read_stream(
        schema,
        spark,
        input_format="json",
        input_path=f"{curr_dir}/tests/data/raw",
        read_options={"multiline": "true"},
    )
    query = write_stream(read_stream_df, output_sink="memory")

    actual_output = spark.table("query_result")
    query.awaitTermination()

    expected_output = (
        spark.read.format("json")
        .schema(schema)
        .option("multiline", "true")
        .load(f"{curr_dir}/tests/data/raw")
    )

    assert_frame_equal(actual_output.toPandas(), expected_output.toPandas())
