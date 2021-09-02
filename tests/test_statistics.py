import json
import os
import shutil
import time
from pyspark.sql.functions import current_date, col
from pandas.testing import assert_frame_equal

from src.statistics import PageUserCount


def test_page_count_stats_pass(spark, tmpdir):
    curr_dir = os.getcwd()
    input_loc = f"{tmpdir}/source"
    with open(f"{curr_dir}/tests/data/raw/test_current.json") as json_file:
        data = json.load(json_file)
    data["timestamp"] = int(time.time() * 1000)

    os.mkdir(input_loc)
    with open(f"{input_loc}/test_current.json", "w") as outfile:
        json.dump(data, outfile, indent="")


    # First Run
    PageUserCount(
        base_dir=tmpdir, schema="pageId string, userId string, timestamp long"
    ).run

    with open(f"{input_loc}/test_current2.json", "w") as outfile:
        json.dump(data, outfile, indent="")

    # Second Run
    PageUserCount(
        base_dir=tmpdir, schema="pageId string, userId string, timestamp long"
    ).run

    df = (
        spark.read.format("delta")
        .load(f"{tmpdir}/page_count_aggregations")
        .filter(col("date") == current_date())
        .orderBy("key")
    )

    expected_page_count_df = spark.createDataFrame(
        [
            ("eec52fd6-3da0-4764-bf5a-bb5dad0f2a0f", 2),
            ("bc6a078c-2718-4131-9726-34d61998321e,eec52fd6-3da0-4764-bf5a-bb5dad0f2a0f", 2)
        ],
        ("key string, daily_count long"),
    )
    expected_page_count_df = (
        expected_page_count_df.withColumn("date", current_date())
        .select("key", "date", "daily_count")
        .orderBy("key")
    )
    assert_frame_equal(df.toPandas(), expected_page_count_df.toPandas())
