import pytest
from delta import *
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark_builder = (
            SparkSession.builder.master("local[8]")
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.streaming.concurrentJobs","2")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
            
    spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()
        
    return spark