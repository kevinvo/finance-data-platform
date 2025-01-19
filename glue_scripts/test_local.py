from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date
import boto3

# Initialize Spark with AWS Hadoop configuration
spark = (
    SparkSession.builder.appName("FinanceDataProcessingTest")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# Set AWS credentials from environment
session = boto3.Session()
credentials = session.get_credentials()
spark.conf.set("spark.hadoop.fs.s3a.access.key", credentials.access_key)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", credentials.secret_key)
if credentials.token:
    spark.conf.set("spark.hadoop.fs.s3a.session.token", credentials.token)

# Define bucket names (these should match your stack)
raw_bucket = "financedataplatform-raw-data"
processed_bucket = "financedataplatform-processed-data"

# Read Yahoo Finance data
yahoo_finance_path = f"s3a://{raw_bucket}/market_data/"
yahoo_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .parquet(yahoo_finance_path)
)

# Read EDGAR Finance data
edgar_finance_path = f"s3a://{raw_bucket}/raw/company_financials/"
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

# Show sample data
print("\nYahoo Finance Sample:")
yahoo_df.show(5)
print("\nEDGAR Finance Sample:")
edgar_df.show(5)

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

# Show results
print("\nCombined Data Sample:")
combined_df.show(5)

# Print statistics
print("\nData Processing Statistics:")
print(f"Total records: {combined_df.count()}")
print(f"Unique companies: {combined_df.select('ticker').distinct().count()}")
print(
    f"Date range: {combined_df.agg({'date': 'min'}).collect()[0][0]} to {combined_df.agg({'date': 'max'}).collect()[0][0]}"
)

spark.stop()
