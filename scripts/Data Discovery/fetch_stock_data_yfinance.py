# fetch_stock_data_yfinance.py

import yfinance as yf
import os


def fetch_stock_data(ticker, start_date, end_date, save_dir="data"):
    """
    Fetch stock data for a given ticker and save it as a CSV file.

    Parameters:
    - ticker (str): Stock ticker symbol (e.g., "AAPL" for Apple).
    - start_date (str): Start date in YYYY-MM-DD format.
    - end_date (str): End date in YYYY-MM-DD format.
    - save_dir (str): Directory to save the CSV file. Default is "data".
    """
    try:
        # Fetch data
        print(f"Fetching data for {ticker} from {start_date} to {end_date}...")
        data = yf.download(ticker, start=start_date, end=end_date)
        print(data.columns)
        # Create directory if it doesn't exist
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # Save to CSV
        file_path = os.path.join(save_dir, f"{ticker}_stock_data.csv")
        data.to_csv(file_path)
        print(f"Data saved to {file_path}")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # User inputs
    ticker = input("Enter stock ticker (e.g., AAPL): ").upper()
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")

    # Fetch and save stock data
    fetch_stock_data(ticker, start_date, end_date)
