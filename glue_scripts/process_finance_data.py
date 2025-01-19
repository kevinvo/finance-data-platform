import sys
import subprocess
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit, to_date

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME", "TempDir"])
job.init(args["JOB_NAME"], args)

# Define bucket names (these should match your stack)
raw_bucket = "financedataplatform-raw-data"
processed_bucket = "financedataplatform-processed-data"

# Read Yahoo Finance data
yahoo_finance_path = f"s3://{raw_bucket}/market_data/"
yahoo_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .parquet(yahoo_finance_path)
)

# Read EDGAR Finance data
edgar_finance_path = f"s3://{raw_bucket}/company_financials/"
edgar_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .parquet(edgar_finance_path)
)

# Process Yahoo Finance data
yahoo_df = yahoo_df.select(
    col("symbol").alias("ticker"),
    col("date"),
    col("open"),
    col("high"),
    col("low"),
    col("close"),
    col("volume"),
    col("dividends"),
    col("stock_splits"),
)

# Process EDGAR Finance data
edgar_df = edgar_df.select(
    col("symbol").alias("ticker"),
    col("filing_date").alias("date"),
    col("total_assets"),
    col("total_liabilities"),
    col("total_equity"),
    col("net_income"),
    col("revenue"),
)

# Convert date columns to the same format
yahoo_df = yahoo_df.withColumn("date", to_date(col("date")))
edgar_df = edgar_df.withColumn("date", to_date(col("date")))

# Join the datasets
combined_df = yahoo_df.join(
    edgar_df,
    (yahoo_df.ticker == edgar_df.ticker) & (yahoo_df.date == edgar_df.date),
    "outer",
)

# Add source column and handle duplicates
combined_df = combined_df.select(
    col("ticker"),
    col("date"),
    # Market data
    col("open"),
    col("high"),
    col("low"),
    col("close"),
    col("volume"),
    col("dividends"),
    col("stock_splits"),
    # Financial data
    col("total_assets"),
    col("total_liabilities"),
    col("total_equity"),
    col("net_income"),
    col("revenue"),
    # Add data source indicator
    when(col("close").isNotNull(), lit("yahoo"))
    .when(col("total_assets").isNotNull(), lit("edgar"))
    .otherwise(lit("unknown"))
    .alias("data_source"),
)

# Write the combined dataset
output_path = f"s3://{processed_bucket}/combined_finance_data"
combined_df.write.mode("overwrite").partitionBy("ticker", "date").parquet(output_path)

# Print some statistics
print("Data Processing Statistics:")
print(f"Total records: {combined_df.count()}")
print(f"Unique companies: {combined_df.select('ticker').distinct().count()}")
print(
    f"Date range: {combined_df.agg({'date': 'min'}).collect()[0][0]} to {combined_df.agg({'date': 'max'}).collect()[0][0]}"
)

# Install dependencies
def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


# Install dependencies
install_package("requests==2.31.0")
install_package("yfinance==0.2.36")

job.commit()
