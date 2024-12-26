from decimal import Decimal
import yfinance as yf
import pandas as pd
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Configure DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

def check_and_create_table(table_name):
    """
    Check if a DynamoDB table exists. If not, create the table.
    :param table_name: Name of the DynamoDB table
    """
    try:
        existing_tables = dynamodb.meta.client.list_tables()["TableNames"]
        if table_name in existing_tables:
            print(f"Table '{table_name}' already exists.")
        else:
            print(f"Creating table '{table_name}'...")
            table = dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "ticker", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "date", "KeyType": "RANGE"}   # Sort key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "ticker", "AttributeType": "S"},
                    {"AttributeName": "date", "AttributeType": "S"}
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5
                }
            )
            table.wait_until_exists()
            print(f"Table '{table_name}' created successfully.")
    except NoCredentialsError:
        print("AWS credentials not found.")
    except ClientError as e:
        print(f"Error checking or creating table: {e}")


def save_to_dynamodb(table_name, ticker, date, data):
    """
    Save stock data to DynamoDB.
    :param table_name: Name of the DynamoDB table
    :param ticker: Stock ticker symbol (e.g., 'AAPL')
    :param date: Date of the stock data
    :param data: Dictionary containing stock market data
    """
    try:
        table = dynamodb.Table(table_name)
        table.put_item(
            Item={
                'ticker': ticker,
                'date': date,
                **data
            }
        )
        print(f"Saved data for {ticker} on {date} to DynamoDB.")
    except Exception as e:
        print(f"Error saving data for {ticker} on {date}: {e}")


def download_and_store_stock_data(table_name, ticker, start_date, end_date):
    """
    Download stock market data using yfinance and save it to DynamoDB.
    :param table_name: Name of the DynamoDB table
    :param ticker: Stock ticker symbol (e.g., 'AAPL')
    :param start_date: Start date for the data (YYYY-MM-DD)
    :param end_date: End date for the data (YYYY-MM-DD)
    """
    # Ensure the table exists
    check_and_create_table(table_name)

    # Download stock data
    try:
        print(f"Downloading stock data for {ticker} from {start_date} to {end_date}...")
        data = yf.download(ticker, start=start_date, end=end_date)

        # Iterate through the data and save each row to DynamoDB
        for index, row in data.iterrows():
            date = index.strftime('%Y-%m-%d')  # Convert datetime to string
            item = {
                'Open': Decimal(str((row[('Open', ticker)]))) if not pd.isna(row[('Open', ticker)]) is None else None,
                'High': Decimal(str((row[('High', ticker)]))) if not pd.isna(row[('High', ticker)]) is None else None,
                'Low': Decimal(str((row[('Low', ticker)]))) if not pd.isna(row[('Low', ticker)]) is None else None,
                'Close': Decimal(str((row[('Close', ticker)]))) if not pd.isna(row[('Close', ticker)]) is None else None,
                'Volume': Decimal(str((row[('Volume', ticker)]))) if not pd.isna(row[('Volume', ticker)]) is None else None
            }
            print(item)
            save_to_dynamodb(table_name, ticker, date, item)

        print(f"Completed saving stock data for {ticker}.")

    except Exception as e:
        print(f"Error downloading or saving stock data for {ticker}: {e}")


if __name__ == "__main__":
    # Parameters
    table_name = "StockMarketData"
    ticker = "MSFT"
    start_date = "2023-01-01"
    end_date = "2023-12-31"

    # Download and store stock data
    download_and_store_stock_data(table_name, ticker, start_date, end_date)
