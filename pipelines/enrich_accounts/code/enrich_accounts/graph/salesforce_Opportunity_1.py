from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.functions import *

def salesforce_Opportunity_1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Id", StringType(), True), StructField("IsDeleted", IntegerType(), True), StructField("AccountId", StringType(), True), StructField("IsPrivate", IntegerType(), True), StructField("Name", StringType(), True), StructField("Description", StringType(), True), StructField("StageName", StringType(), True), StructField("StageSortOrder", IntegerType(), True), StructField("Amount", IntegerType(), True), StructField("Probability", IntegerType(), True), StructField("ExpectedRevenue", IntegerType(), True), StructField("TotalOpportunityQuantity", StringType(), True), StructField("CloseDate", TimestampType(), True), StructField("Type", StringType(), True), StructField("NextStep", StringType(), True), StructField("LeadSource", StringType(), True), StructField("IsClosed", IntegerType(), True), StructField("IsWon", IntegerType(), True), StructField("ForecastCategory", StringType(), True), StructField("ForecastCategoryName", StringType(), True), StructField("CampaignId", StringType(), True), StructField("HasOpportunityLineItem", StringType(), True), StructField("Pricebook2Id", StringType(), True), StructField("OwnerId", StringType(), True), StructField("CreatedDate", TimestampType(), True), StructField("CreatedById", StringType(), True), StructField("LastModifiedDate", TimestampType(), True), StructField("LastModifiedById", StringType(), True), StructField("SystemModstamp", TimestampType(), True), StructField("LastActivityDate", StringType(), True), StructField("LastStageChangeDate", StringType(), True), StructField("FiscalYear", IntegerType(), True), StructField("FiscalQuarter", IntegerType(), True), StructField("ContactId", StringType(), True), StructField("PrimaryPartnerAccountId", StringType(), True), StructField("ContractId", StringType(), True), StructField("LastAmountChangedHistoryId", StringType(), True), StructField("LastCloseDateChangedHistoryId", StringType(), True), StructField("DeliveryInstallationStatus__c", StringType(), True), StructField("TrackingNumber__c", LongType(), True), StructField("OrderNumber__c", IntegerType(), True), StructField("CurrentGenerators__c", StringType(), True), StructField("MainCompetitors__c", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/salesforce_export/Opportunity.csv")
