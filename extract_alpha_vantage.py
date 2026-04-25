import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import pandas_gbq
from google.oauth2 import service_account
import time
import json 

# load environment variables
load_dotenv()
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# target assets
TICKERS = ["SPLG", "VXUS", "SPTM"]

def fetch_and_flatten(ticker):
    """Fetches daily time series data from Alpha Vantage API 
    then transforms the nested JSON into a pandas dataframe
    """
    
    # Alpha Vantage endpoint for "TIME_SERIES_DAILY" function
    # use the ticker arg to build a custom url
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={API_KEY}"
    response = requests.get(url)
    
    # check if request was successful 
    if response.status_code != 200:
        print(f"Error fetching {ticker}: HTTP {response.status_code}")
        return pd.DataFrame()
        
    data = response.json()
    
    # while a 200 status means server successfully received message, it does 
    # not mean the request was valid or that data recieved 
    if "Error Message" in data:
        print(f"API Error for {ticker}: {data['Error Message']}")
    if "Note" in data or "Information" in data:
        print(f"Rate Limit Reached: {data.get('Note', data.get('Information'))}")

    # the actual daily data (prices)
    time_series = data.get("Time Series (Daily)", {})
    
    # flatten nested json into rows
    # example item: '2026-02-04': {'1. open': '81.0150', '2. high': '81.0550'...}
    records = []
    for date_str, metrics in time_series.items():
        records.append({
            "ticker": ticker,
            "market_date": datetime.strptime(date_str, '%Y-%m-%d').date(), 
            "open_price": float(metrics["1. open"]),
            "high_price": float(metrics["2. high"]),
            "low_price": float(metrics["3. low"]),
            "close_price": float(metrics["4. close"]),
            "volume": int(metrics["5. volume"]),
            # needed to track when data was pulled
            "load_timestamp": datetime.now()
        })
        
    return pd.DataFrame(records)

def main():
    print("Starting data extraction...")
    all_dataframes = []
    
    for i, ticker in enumerate(TICKERS):
        # if it's not the first ticker, wait before fetching the next one
        # helps not hit rate limit 
        if i > 0:
            time.sleep(15)
        
        print(f"Fetching data for {ticker}...")
        df = fetch_and_flatten(ticker)
        
        if not df.empty:
            all_dataframes.append(df)
            
    # combine all individual ticker dataframes into one master dataframe
    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        print(f"\nTotal rows ready for BigQuery: {len(final_df)}")
        
        project_id = 'analytics-dev-493823' 
        
        # format: dataset_name.table_name
        destination_table = 'raw_data.daily_stock_prices' 
        
        print(f"Loading data to BigQuery table: {destination_table}...")
        
        try:
            # check for google credentials json string in environment variables (github actions)
            sa_key_string = os.getenv("GCP_SA_KEY")
            
            if sa_key_string:
                # parse the json string into a dictionary
                credentials_dict = json.loads(sa_key_string)
                credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            else:
                # retrieve path to the service account json file
                key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                
                # create credentials object using json key
                credentials = service_account.Credentials.from_service_account_file(key_path)
            
            # use pandas-gbq to automate schema mapping, handle BQ job submission, 
            # and append new records 
            pandas_gbq.to_gbq(
                final_df, 
                destination_table=destination_table,
                project_id=project_id,
                credentials=credentials,
                if_exists='replace'
            )
            print("Successfully loaded to BigQuery!")
        except Exception as e:
            print(f"Failed to load to BigQuery. Error: {e}")
            
    else:
        print("No data was extracted.")

if __name__ == "__main__":
    main()