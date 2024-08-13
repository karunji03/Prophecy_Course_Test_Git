from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.functions import *

def flatten_schema(spark: SparkSession, in0: DataFrame) -> DataFrame:
    flt_col = in0.columns
    selectCols = [col("Id") if "Id" in flt_col else col("Id"),                   col("IsDeleted") if "IsDeleted" in flt_col else col("IsDeleted"),                   col("MasterRecordId") if "MasterRecordId" in flt_col else col("MasterRecordId")]

    return in0.select(*selectCols)
