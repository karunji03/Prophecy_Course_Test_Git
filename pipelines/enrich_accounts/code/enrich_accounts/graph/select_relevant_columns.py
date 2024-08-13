from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.functions import *

def select_relevant_columns(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Id"), 
        col("IsDeleted"), 
        col("MasterRecordId"), 
        col("Name"), 
        col("Type"), 
        col("BillingCountry")
    )
