import sys
import subprocess
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit, to_date, coalesce

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
yahoo_finance_path = f"s3://{raw_bucket}/raw/market_data/"
yahoo_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .json(yahoo_finance_path)
)

# Read EDGAR Finance data
edgar_finance_path = f"s3://{raw_bucket}/raw/company_financials/"
edgar_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .json(edgar_finance_path)
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
    col("daily_return"),
    col("fetch_timestamp"),
    col("hour"),
)

# Process EDGAR Finance data
edgar_df = edgar_df.select(
    col("symbol").alias("ticker"),
    col("filing_date").alias("date"),
    col("company_name"),
    col("filing_type"),
    col("filing_url"),
    col("fetch_timestamp"),
)

# Convert date columns to the same format
yahoo_df = yahoo_df.withColumn("date", to_date(col("date")))
edgar_df = edgar_df.withColumn("date", to_date(col("date")))

# Join the datasets
combined_df = yahoo_df.join(
    edgar_df,
    (yahoo_df.ticker == edgar_df.ticker) & (yahoo_df.date == edgar_df.date),
    "outer",
).select(
    yahoo_df.ticker.alias("ticker"),
    coalesce(yahoo_df.date, edgar_df.date).alias("date"),
    # Market data
    col("open"),
    col("high"),
    col("low"),
    col("close"),
    col("volume"),
    col("daily_return"),
    yahoo_df.fetch_timestamp.alias("market_data_timestamp"),
    col("hour"),
    # Financial data
    col("company_name"),
    col("filing_type"),
    col("filing_url"),
    edgar_df.fetch_timestamp.alias("filing_timestamp"),
    # Add data source indicator
    when(col("close").isNotNull(), lit("yahoo"))
    .when(col("filing_type").isNotNull(), lit("edgar"))
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
