from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Dict, Any, Optional
import yfinance as yf
import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass
class StockPrice:
    symbol: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    daily_return: Optional[float] = None
    fetch_timestamp: Optional[str] = None


@dataclass
class StockInfo:
    symbol: str
    name: Optional[str]
    sector: Optional[str]
    industry: Optional[str]
    market_cap: Optional[float]
    pe_ratio: Optional[float]
    dividend_yield: Optional[float]
    fetch_timestamp: str
    error: Optional[str] = None


@dataclass
class ProcessingResult:
    symbol: str
    status: str
    records: Optional[int] = None
    error: Optional[str] = None
    stock_info: Optional[StockInfo] = None


class YahooFinanceETL:
    def __init__(self, bucket_name: str):
        """Initialize the ETL class with S3 bucket configuration."""
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")

    def fetch_stock_data(self, symbol: str, period: str = "1d") -> List[StockPrice]:
        """
        Fetch stock data for a given symbol.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            period: Data period to fetch (e.g., '1d', '5d', '1mo')

        Returns:
            List of StockPrice objects
        """
        try:
            logger.info(f"Fetching data for {symbol}")
            ticker = yf.Ticker(symbol)
            history = ticker.history(period=period)

            stock_prices = []
            prev_close = None

            for index, row in history.iterrows():
                # Calculate daily return
                daily_return = None
                if prev_close is not None and prev_close != 0:
                    daily_return = (row["Close"] - prev_close) / prev_close * 100
                prev_close = row["Close"]

                stock_price = StockPrice(
                    symbol=symbol,
                    date=index.strftime("%Y-%m-%d %H:%M:%S"),
                    open=row["Open"],
                    high=row["High"],
                    low=row["Low"],
                    close=row["Close"],
                    volume=row["Volume"],
                    daily_return=daily_return,
                    fetch_timestamp=datetime.now().isoformat(),
                )
                stock_prices.append(stock_price)

            return stock_prices

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            # raise

    def write_to_s3(self, stock_prices: List[StockPrice], symbol: str) -> None:
        try:
            # Convert to list of dictionaries
            data = [asdict(price) for price in stock_prices]
            json_data = json.dumps(data, default=str)

            # Generate S3 key with partitioning
            current_date = datetime.now().strftime("%Y-%m-%d")
            current_hour = datetime.now().strftime("%H")
            s3_key = f"raw/market_data/symbol={symbol}/date={current_date}/hour={current_hour}/data.json"

            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=s3_key, Body=json_data
            )

            logger.info(f"Successfully wrote data for {symbol} to {s3_key}")

        except Exception as e:
            logger.error(f"Error writing to S3 for {symbol}: {str(e)}")
            # raise

    def get_stock_info(self, symbol: str) -> StockInfo:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            return StockInfo(
                symbol=symbol,
                name=info.get("longName"),
                sector=info.get("sector"),
                industry=info.get("industry"),
                market_cap=info.get("marketCap"),
                pe_ratio=info.get("trailingPE"),
                dividend_yield=info.get("dividendYield"),
                fetch_timestamp=datetime.now().isoformat(),
            )

        except Exception as e:
            logger.error(f"Error fetching info for {symbol}: {str(e)}")
            return StockInfo(
                symbol=symbol,
                name=None,
                sector=None,
                industry=None,
                market_cap=None,
                pe_ratio=None,
                dividend_yield=None,
                fetch_timestamp=datetime.now().isoformat(),
                error=str(e),
            )


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for fetching Yahoo Finance data.

    Event format:
    {
        "symbols": ["AAPL", "MSFT", "GOOGL"],
        "period": "1d"  # optional
    }
    """
    try:
        # Get environment variables
        bucket_name = os.environ["BUCKET_NAME"]

        # Get parameters from event
        symbols = event.get("symbols", ["AAPL", "MSFT", "GOOGL"])
        period = event.get("period", "1d")

        # Initialize ETL class
        etl = YahooFinanceETL(bucket_name)

        # Process each symbol
        results: List[ProcessingResult] = []
        for symbol in symbols:
            try:
                # Fetch and write stock data
                stock_prices = etl.fetch_stock_data(symbol, period)
                etl.write_to_s3(stock_prices, symbol)

                # Fetch and store additional info
                stock_info = etl.get_stock_info(symbol)

                results.append(
                    ProcessingResult(
                        symbol=symbol,
                        status="success",
                        records=len(stock_prices),
                        stock_info=stock_info,
                    )
                )

            except Exception as e:
                results.append(
                    ProcessingResult(symbol=symbol, status="error", error=str(e))
                )
                continue

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": f"Processed {len(symbols)} symbols",
                    "results": [asdict(result) for result in results],
                },
                default=str,
            ),
        }

    except Exception as e:
        logger.error(f"Error in handler: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


if __name__ == "__main__":
    # For local testing
    test_event = {"symbols": ["AAPL", "MSFT", "GOOGL"], "period": "1d"}
    print(handler(test_event, None))
