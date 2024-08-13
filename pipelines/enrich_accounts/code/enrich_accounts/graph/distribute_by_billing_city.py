from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.functions import *

def distribute_by_billing_city(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
    df1 = in0.filter((col("BillingCity") == lit("Null")))
    df2 = in0.filter((col("BillingCity") != lit("Null")))

    return df1, df2
