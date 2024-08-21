import os

import xxhash
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def to_xxhash(x: int) -> str:
    hashed = str(xxhash.xxh64_intdigest(str(x)))

    return hashed


def test_spark():
    java_home = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", ".pixi/envs/default/lib/jvm"))
    os.environ["JAVA_HOME"] = java_home

    spark = (
        SparkSession.builder.appName("spark test")
        .master("local[1]")
        .getOrCreate()
    )

    my_udf = udf(to_xxhash, returnType=StringType())

    spark.range(1, 5).withColumn("id", my_udf("id")).show()
