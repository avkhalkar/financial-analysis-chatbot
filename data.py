import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timezone
from typing import Literal, Dict, Any, cast

STORAGE_DIR = "yfdata" # New directory for storing data files
os.makedirs(STORAGE_DIR, exist_ok=True)

def fetch_and_store_stock_data(
    ticker: str, 
    report_type: Literal["price", "income_stmt", "balance_sheet", "cash_flow", "info"] = "price"
) -> str:
    """
    Fetches financial data, enriches it with metadata (timestamp, source), 
    and saves it as a Parquet file in the CURRENT directory.
    """
    print(f"Fetching {report_type} for {ticker}...")
    
    ticker = ticker.strip().upper()
    stock = yf.Ticker(ticker)
    
    try:
        df = pd.DataFrame() 
        
        if report_type == "price":
            df = stock.history(period="2mo", interval="2d")
            if df.empty:
                raise ValueError(f"No price data found for {ticker}")
            
            df = df.iloc[::2]
            df.reset_index(inplace=True)


        elif report_type == "income_stmt":
            df = stock.financials.T 
            df.reset_index(inplace=True)
            df.rename(columns={"index": "Date"}, inplace=True)
        
        elif report_type == "balance_sheet":
            df = stock.balance_sheet.T
            df.reset_index(inplace=True)
            df.rename(columns={"index": "Date"}, inplace=True)
        
        elif report_type == "cash_flow":
            df = stock.cashflow.T
            df.reset_index(inplace=True)
            df.rename(columns={"index": "Date"}, inplace=True)

        elif report_type == "info":
            info_dict = stock.info
            keys_to_keep = [
                'longName', 'sector', 'industry', 'marketCap', 
                'forwardPE', 'dividendYield', 'profitMargins', 'totalRevenue'
            ]
            # Use .get() to handle missing keys
            filtered_info = {k: [info_dict.get(k, None)] for k in keys_to_keep}
            df = pd.DataFrame(filtered_info)

        else:
            raise ValueError(f"Invalid report_type: {report_type}")

        # Metadata
        current_time = datetime.now(timezone.utc)
        
        df["_meta_ticker"] = ticker
        df["_meta_report_type"] = report_type
        df["_meta_source"] = "yfinance"
        df["_meta_fetched_at"] = current_time
        df["_meta_data_version"] = "v1.0"

        filename = f"{ticker}_{report_type}_{current_time.strftime('%Y%m%d')}.parquet"
        
        file_path = os.path.join(STORAGE_DIR, filename)
        
        # Save to Parquet
        df.to_parquet(file_path, index=False)
        
        print(f"✅ Successfully stored {report_type} for {ticker} at: {file_path}")
        return file_path

    except Exception as e:
        error_msg = f"Error processing {ticker} ({report_type}): {str(e)}"
        print(error_msg)
        return ""

if __name__ == "__main__":
    watchlist = ["AAPL", "MSFT"]
    required_reports = ["price", "income_stmt", "info"]

    print(f"Starting Data Ingestion Job for {len(watchlist)} companies...\n")

    generated_files = []

    for symbol in watchlist:
        for report in required_reports:
            path = fetch_and_store_stock_data(symbol, cast(Literal["price", "income_stmt", "balance_sheet", "cash_flow", "info"], report))
            if path:
                generated_files.append(path)
    
    print(f"\n Job Complete. {len(generated_files)} files generated in current directory.")
