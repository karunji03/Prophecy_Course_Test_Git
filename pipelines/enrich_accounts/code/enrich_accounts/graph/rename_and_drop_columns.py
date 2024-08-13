from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.functions import *

def rename_and_drop_columns(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.withColumnRenamed("Name", "Customer_Name")

    return df1.drop("Jigsaw")
