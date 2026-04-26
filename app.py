import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import pandas as pd

# sets browser tab title and forces app to use full width of screen
st.set_page_config(page_title="Global Equity Correlation Monitor", layout="wide")
st.title("Global Equity Correlation Monitor")
st.markdown("Analyzing diversification and asset relationships across US and International equities.")
with st.expander("Why these 5 ETFs?"):
    st.write("""
    This app compares five different types of investments to see how they move in relation to one another. 
    
    * **QQQ (Big Tech):** It tracks the 100 largest tech-focused companies.
    * **GLD (Gold):** Gold often holds its value or goes up when the stock market is struggling.
    * **KRE (Local Banks):** These are sensitive to interest rates, often behaving differently than the big tech giants.
    * **EEM (Emerging Markets):** Focuses on fast-growing countries like India and Brazil, which follow their own unique economic paths.
    * **ARKK (Moonshots):** It invests in experimental tech (like robotics), showing us the difference between steady growth and high volatility.
    
    **The Goal:** To determine which assets provide true diversification.
    """)

# decorator: run the following function once and keep the output (the database connection)
@st.cache_resource
def get_bq_client():
    """secure cloud authentication"""
    credentials_info = st.secrets["gcp_service_account"]  # pull secrets
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return bigquery.Client(credentials=credentials, project=credentials_info["project_id"])

# decorator: save the resulting dataframe in memory and serve to anyone who visits 
# in next 24 hours
@st.cache_data(ttl="24h")
def load_data():
    client = get_bq_client()  # connection
    query = """
        SELECT ticker, close_price, market_date, load_timestamp
        FROM `analytics-dev-493823.raw_data.daily_stock_prices`
        ORDER BY market_date DESC
    """
    df = client.query(query).to_dataframe()
    df['market_date'] = pd.to_datetime(df['market_date'])
    df['load_timestamp'] = pd.to_datetime(df['load_timestamp'])
    return df

df = load_data()

if not df.empty:
    # Sidebar Filtering
    st.sidebar.header("Navigation & Filters")
    symbols = st.sidebar.multiselect(
        "Select Funds to Compare", 
        options=sorted(df['ticker'].unique()), 
        default=sorted(df['ticker'].unique())
    )
    
    st.sidebar.markdown("---")
    st.sidebar.info("""
    **Interpreting Results:**
    - **Correlation > 0.80:** Assets move in lockstep.
    - **Correlation < 0.50:** Strong diversification benefits.
    """)
    
    filtered_df = df[df['ticker'].isin(symbols)]

    # for calculating daily performance
    latest_date = df['market_date'].max() # most recent date
    prev_date = df[df['market_date'] < latest_date]['market_date'].max() # next most recent
    
    # tells Streamlit to divide the horizontal space of the app into equal parts 
    # (dynamic based on number of symbols)
    metric_cols = st.columns(len(symbols) if len(symbols) > 0 else 1)
    
    for i, ticker in enumerate(symbols):
        ticker_data = df[df['ticker'] == ticker]  # only rows for ticker data
        curr_val = ticker_data[ticker_data['market_date'] == latest_date]['close_price'].values[0]
        prev_val = ticker_data[ticker_data['market_date'] == prev_date]['close_price'].values[0]
        delta = ((curr_val - prev_val) / prev_val) * 100  # percent change
        
        # place this kpi card into the i column
        metric_cols[i].metric(
            label=ticker, 
            value=f"${curr_val:,.2f}", 
            delta=f"{delta:.2f}%"
        )

    st.markdown("---")

    # ==========================================
    # MAIN CONTENT: TABBED VIEW 
    # ==========================================
    tab1, tab2, tab3 = st.tabs(["Price Performance", "Correlation Analysis", "Raw Data"])

    with tab1:
        st.subheader("Historical Closing Prices")
        fig_line = px.line(
            filtered_df, 
            x="market_date", 
            y="close_price", 
            color="ticker",
            template="plotly_white",
            labels={"market_date": "Date", "close_price": "Price (USD)", "ticker": "ETF"}
        )
        fig_line.update_layout(
            hovermode="x unified",
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig_line, width='stretch')  # bridge between plotly and streamlit

    with tab2:
        st.subheader("Daily Returns Correlation")
        
        # reshape data so market_date becomes index and each ticker becomes its own column
        pivot_df = filtered_df.pivot(index='market_date', columns='ticker', values='close_price')

        returns_df = pivot_df.pct_change().dropna() # daily pct change (first day will be null)
        corr_matrix = returns_df.corr()

        fig_corr = px.imshow(
            corr_matrix, 
            text_auto=".2f",
            color_continuous_scale='RdBu_r', 
            zmin=-1, zmax=1,
            template="plotly_white"
        )
        fig_corr.update_layout(
            coloraxis_showscale=False,  # no color bar
            margin=dict(l=0, r=0, t=30, b=0)  # margins
        )
        st.plotly_chart(fig_corr, width='stretch')  # bridge between plotly and streamlit

    with tab3:
        st.subheader("Dataset Preview")
        st.dataframe(filtered_df, width='stretch')

    # pipeline health
    st.divider() 
    pacific_time = df['load_timestamp'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
    last_sync_pst = pacific_time.max().strftime('%Y-%m-%d %I:%M:%S %p')
    st.caption(f"⚙️ **Pipeline Status:** Last data sync completed at {last_sync_pst} PT via automated GitHub Actions ETL.")
    
else:
    st.warning("No data found in BigQuery. Check your ingestion pipeline.")