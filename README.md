# Global Equity Correlation Monitor
An end-to-end data pipeline that automates the collection, storage, and visualization of global market data to analyze how different asset classes interact.

[View Live Dashboard](https://global-equity-monitor.streamlit.app/)

### The Workflow
This project is built as a fully automated system with data flowing through a modern cloud architecture:
1. Data Extraction 
The process begins with a Python script (extract_alpha_vantage.py) that communicates with the Alpha Vantage API.
- Fetching: The script requests daily price data for a list of ETFs.
- Cleaning: Nested JSON responses are flattened into a Pandas DataFrame.
- Metadata: Every row is tagged with a "load timestamp" to track exactly when the data entered the system.

2. Data Storage 
Once the data is cleaned, it is moved into the cloud for storage.
- Loading: The pipeline uses the pandas-gbq library to securely upload the data into Google BigQuery.

3. Interactive Visualization 
The final stage is a user-facing dashboard built with Streamlit and Plotly.
- Real-Time Read: The app queries BigQuery directly, ensuring the visuals reflect the latest available data.
- Interactive UI: Users can filter by specific ETFs, view price trends, and explore a correlation matrix that updates dynamically based on their selections.
- Efficiency: Uses localized caching to reduce BigQuery query costs and provide fast chart updates.

### Automation
This entire cycle is managed by GitHub Actions.
- Daily Runs: The system is scheduled to run the extraction script Monday-Friday at 4:30 PM and update the database.
- Secrets Management: Sensitive API keys and Google Cloud credentials are managed securely through GitHub and Streamlit's encrypted secret vaults.

### Technical Stack
- Language: Python 3.12
- Visualization: Streamlit, Plotly 
- Cloud: Google BigQuery, Google Cloud Platform
- Automation: GitHub Actions