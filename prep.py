from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from urllib.parse import urlparse
import re

spark = SparkSession.builder \
    .appName("WebAccessLogProcessing") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

parquet_file = "/content/logs_df.parquet"
df = spark.read.parquet(parquet_file)

def extract_path(request):
    try:
        path=urlparse(request).path
        if path.startswith("/m"):
          return re.findall(r"([^/\\]+)", path)[1]
        else:
          return re.findall(r"([^/\\]+)", path)[0]
    except:
        return None


extract_path_udf = udf(extract_path, StringType())

df = df.withColumn("request", extract_path_udf(df["request"]))

output_file = "/content/final.parquet"
df.coalesce(1).write.parquet(output_file, mode="overwrite")

spark.stop()
