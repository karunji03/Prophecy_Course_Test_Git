from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.functions import *
from prophecy.utils import *
from enrich_accounts.graph import *

def pipeline(spark: SparkSession) -> None:
    df_salesforce_Account = salesforce_Account(spark)
    df_filter_true = filter_true(spark, df_salesforce_Account)
    df_rename_and_drop_columns = rename_and_drop_columns(spark, df_salesforce_Account)
    df_employees_by_billing_city = employees_by_billing_city(spark, df_salesforce_Account)
    df_salesforce_Opportunity = salesforce_Opportunity(spark)
    df_selectfields_Opportunity = selectfields_Opportunity(spark, df_salesforce_Opportunity)
    df_select_relevant_columns = select_relevant_columns(spark, df_salesforce_Account)
    df_aggregate_opportunities_by_account = aggregate_opportunities_by_account(spark, df_selectfields_Opportunity)
    df_account_join = account_join(spark, df_salesforce_Account, df_aggregate_opportunities_by_account)
    enriched_accounts(spark, df_account_join)
    df_flatten_schema = flatten_schema(spark, df_salesforce_Account)
    df_limit_to_two = limit_to_two(spark, df_salesforce_Account)
    df_by_name_asc = by_name_asc(spark, df_salesforce_Account)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("enrich_accounts")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/enrich_accounts")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/enrich_accounts", config = Config)(pipeline)

if __name__ == "__main__":
    main()
