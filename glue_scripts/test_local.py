from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, coalesce
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
yahoo_finance_path = f"s3a://{raw_bucket}/raw/market_data/"
yahoo_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .json(yahoo_finance_path)
)

# Print schemas to see available columns
print("\nYahoo Finance Schema:")
yahoo_df.printSchema()

# Read EDGAR Finance data
edgar_finance_path = f"s3a://{raw_bucket}/raw/company_financials/"
edgar_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .json(edgar_finance_path)
)

print("\nEDGAR Finance Schema:")
edgar_df.printSchema()

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
).select(
    # Use yahoo_df.ticker as the primary ticker
    yahoo_df.ticker.alias("ticker"),
    # Use coalesce to handle null dates from either source
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
