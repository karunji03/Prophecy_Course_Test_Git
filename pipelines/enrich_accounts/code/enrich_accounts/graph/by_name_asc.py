from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.functions import *

def by_name_asc(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("Name").asc())
