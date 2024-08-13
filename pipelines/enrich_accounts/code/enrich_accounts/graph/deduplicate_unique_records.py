from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.functions import *

def deduplicate_unique_records(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("count", count("*").over(Window.partitionBy("Id", "Name")))\
        .filter(col("count") == lit(1))\
        .drop("count")
