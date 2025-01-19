from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests
import json
import logging
import os
from bs4 import BeautifulSoup
import boto3


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass
class FinancialStatement:
    symbol: str
    filing_type: str  # 10-K or 10-Q
    filing_date: str
    period_end_date: str
    assets: Optional[float]
    liabilities: Optional[float]
    revenue: Optional[float]
    net_income: Optional[float]
    eps: Optional[float]
    fetch_timestamp: Optional[str] = None


@dataclass
class CompanyFiling:
    symbol: str
    company_name: str
    filing_type: str
    filing_date: str
    filing_url: str
    fetch_timestamp: Optional[str] = None


@dataclass
class ProcessingResult:
    symbol: str
    status: str
    records: Optional[int] = None
    error: Optional[str] = None
    filings: Optional[List[CompanyFiling]] = None


class EDGARFinanceETL:
    def __init__(self, bucket_name: str):
        """Initialize the ETL class with S3 bucket configuration."""
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")
        self.sec_base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
        self.headers = {
            "User-Agent": "Company Financial Data Collector yourname@email.com"
        }

    def fetch_company_filings(
        self, symbol: str, filing_type: str = "10-Q"
    ) -> List[CompanyFiling]:
        """Fetch company filings from SEC EDGAR."""
        try:
            params = {"CIK": symbol, "type": filing_type, "count": 10}

            response = requests.get(
                self.sec_base_url, params=params, headers=self.headers
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            filing_tables = soup.find_all("table", class_="tableFile2")

            filings = []
            for table in filing_tables:
                rows = table.find_all("tr")
                for row in rows[1:]:
                    cols = row.find_all("td")
                    if len(cols) >= 4:
                        filing = CompanyFiling(
                            symbol=symbol,
                            company_name=soup.find("span", class_="companyName")
                            .text.split("[")[0]
                            .strip(),
                            filing_type=cols[0].text.strip(),
                            filing_date=cols[3].text.strip(),
                            filing_url=f"https://www.sec.gov{cols[1].find('a')['href']}",
                            fetch_timestamp=datetime.now().isoformat(),
                        )
                        filings.append(filing)

            return filings

        except Exception as e:
            logger.error(f"Error fetching filings for {symbol}: {str(e)}")
            return []

    def write_to_s3(self, filings: List[CompanyFiling], symbol: str) -> None:
        """Write company filings to S3."""
        try:
            if not filings:
                return

            data = [asdict(filing) for filing in filings]
            json_data = json.dumps(data, default=str)

            current_date = datetime.now().strftime("%Y-%m-%d")
            s3_key = f"raw/company_financials/symbol={symbol}/date={current_date}/filings.json"

            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=s3_key, Body=json_data
            )

        except Exception as e:
            logger.error(f"Error writing to S3 for {symbol}: {str(e)}")


def handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """
    Lambda handler for fetching SEC EDGAR financial data.
    Can be run both locally and in AWS Lambda.

    Args:
        event: Dictionary with:
            - symbols: List of company symbols
            - filing_type: Type of filing to fetch (optional)
        context: AWS Lambda context (optional)

    Returns:
        Dictionary with status code and results
    """
    try:
        # Get bucket name from environment or use None for local testing
        bucket_name = os.environ.get("BUCKET_NAME")

        # Log the configuration
        logger.info(f"Starting handler with bucket: {bucket_name}")
        logger.info(f"Event: {event}")

        # Get parameters from event
        symbols = event.get("symbols", ["AAPL", "MSFT", "GOOGL"])
        filing_type = event.get("filing_type", "10-Q")

        # Initialize ETL class
        etl = EDGARFinanceETL(bucket_name) if bucket_name else None

        # Process each symbol
        results: List[ProcessingResult] = []
        for symbol in symbols:
            try:
                # Fetch filings
                filings = etl.fetch_company_filings(symbol, filing_type) if etl else []

                # Write to S3 only if bucket is configured
                if etl and bucket_name:
                    etl.write_to_s3(filings, symbol)

                results.append(
                    ProcessingResult(
                        symbol=symbol,
                        status="success",
                        records=len(filings),
                        filings=filings,
                    )
                )

            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
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
    # Example usage
    test_event = {"symbols": ["AAPL", "MSFT", "GOOGL"], "filing_type": "10-Q"}

    # Run the handler
    result = handler(event=test_event)

    # Pretty print the results
    print(json.dumps(json.loads(result["body"]), indent=2))
