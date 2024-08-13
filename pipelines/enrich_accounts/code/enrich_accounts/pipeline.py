from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.functions import *
from prophecy.utils import *
from enrich_accounts.graph import *

def pipeline(spark: SparkSession) -> None:
    df_salesforce_Account_1 = salesforce_Account_1(spark)
    df_salesforce_Opportunity_1 = salesforce_Opportunity_1(spark)
    df_left_outer_join_by_id = left_outer_join_by_id(spark, df_salesforce_Account_1, df_salesforce_Opportunity_1)
    df_salesforce_Account = salesforce_Account(spark)
    df_column_comparison_summary = column_comparison_summary(spark, df_salesforce_Account_1, df_salesforce_Account)
    df_filter_true = filter_true(spark, df_salesforce_Account)
    df_rename_and_drop_columns = rename_and_drop_columns(spark, df_salesforce_Account)
    df_employees_by_billing_city = employees_by_billing_city(spark, df_salesforce_Account)
    df_salesforce_Opportunity = salesforce_Opportunity(spark)
    df_selectfields_Opportunity = selectfields_Opportunity(spark, df_salesforce_Opportunity)
    df_select_relevant_columns = select_relevant_columns(spark, df_salesforce_Account)
    df_aggregate_opportunities_by_account = aggregate_opportunities_by_account(spark, df_selectfields_Opportunity)
    df_account_join = account_join(spark, df_salesforce_Account, df_aggregate_opportunities_by_account)
    enriched_accounts(spark, df_account_join)
    df_deduplicate_unique_records = deduplicate_unique_records(spark, df_salesforce_Account)
    df_flatten_schema = flatten_schema(spark, df_salesforce_Account)
    df_WindowFunction_1 = WindowFunction_1(spark)
    df_limit_to_two = limit_to_two(spark, df_salesforce_Account)
    df_SetOperation_1 = SetOperation_1(spark, df_salesforce_Account)
    df_distribute_by_billing_city_out0, df_distribute_by_billing_city_out1 = distribute_by_billing_city(
        spark, 
        df_salesforce_Account
    )
    df_by_name_asc = by_name_asc(spark, df_salesforce_Account)
    df_Repartition_1 = Repartition_1(spark, df_salesforce_Account)
    df_Reformat_1 = Reformat_1(spark, df_distribute_by_billing_city_out0)

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
