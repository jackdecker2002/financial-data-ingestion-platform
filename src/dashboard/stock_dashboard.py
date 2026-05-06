import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.storage.postgres_client import PostgresClient

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "finance",
    "user": "postgres",
    "password": "postgres",
}

def load_stock_data(symbol=None, start_date=None, end_date=None):
    """Load stock data from PostgreSQL with optional filters."""
    with PostgresClient(DB_CONFIG) as db:
        query = "SELECT * FROM raw_stock_prices WHERE 1=1"
        params = []
        
        if symbol:
            query += " AND symbol = %s"
            params.append(symbol)
        
        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)
        
        query += " ORDER BY date DESC"
        
        data = db.fetch_all(query, tuple(params))
        if data:
            df = pd.DataFrame(data, columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'ingested_at'])
        else:
            df = pd.DataFrame(columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'ingested_at'])
        return df

def load_pipeline_state():
    """Load pipeline state information."""
    with PostgresClient(DB_CONFIG) as db:
        data = db.fetch_all("SELECT * FROM pipeline_state")
        if data:
            df = pd.DataFrame(data, columns=['table_name', 'symbol', 'last_watermark', 'updated_at'])
        else:
            df = pd.DataFrame(columns=['table_name', 'symbol', 'last_watermark', 'updated_at'])
        return df

def load_pipeline_runs():
    """Load recent pipeline runs."""
    with PostgresClient(DB_CONFIG) as db:
        data = db.fetch_all("SELECT * FROM pipeline_runs ORDER BY start_time DESC LIMIT 10")
        if data:
            df = pd.DataFrame(data, columns=['run_id', 'start_time', 'end_time', 'status'])
        else:
            df = pd.DataFrame(columns=['run_id', 'start_time', 'end_time', 'status'])
        return df

def create_price_chart(df, symbol):
    """Create interactive price chart."""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    fig.add_trace(go.Candlestick(
        x=df['date'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name=f'{symbol} Price'
    ))
    
    fig.update_layout(
        title=f'{symbol} Stock Price',
        yaxis_title='Price ($)',
        xaxis_title='Date',
        template='plotly_white',
        height=600
    )
    
    return fig

def create_volume_chart(df, symbol):
    """Create volume chart."""
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['date'],
        y=df['volume'],
        name='Volume',
        marker_color='lightblue'
    ))
    
    fig.update_layout(
        title=f'{symbol} Trading Volume',
        yaxis_title='Volume',
        xaxis_title='Date',
        template='plotly_white',
        height=300
    )
    
    return fig

def main():
    st.set_page_config(
        page_title="Financial Data Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Financial Data Ingestion Dashboard")
    st.markdown("---")
    
    # Sidebar for filters
    st.sidebar.header("📈 Data Filters")
    
    # Get available symbols
    all_data = load_stock_data()
    available_symbols = ["All"] + list(all_data['symbol'].unique()) if not all_data.empty else ["All"]
    
    symbol_filter = st.sidebar.selectbox("Select Symbol", available_symbols)
    
    # Date range filter
    min_date = all_data['date'].min() if not all_data.empty else datetime.now() - timedelta(days=365)
    max_date = all_data['date'].max() if not all_data.empty else datetime.now()
    
    start_date = st.sidebar.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
    end_date = st.sidebar.date_input("End Date", max_date, min_value=min_date, max_value=max_date)
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.rerun()
    
    # Main content
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📈 Stock Price Charts")
        
        # Load filtered data
        symbol = None if symbol_filter == "All" else symbol_filter
        df = load_stock_data(symbol, start_date, end_date)
        
        if not df.empty:
            if symbol:
                # Single symbol view
                fig_price = create_price_chart(df, symbol)
                st.plotly_chart(fig_price, use_container_width=True)
                
                fig_volume = create_volume_chart(df, symbol)
                st.plotly_chart(fig_volume, use_container_width=True)
                
                # Key metrics
                st.subheader("📊 Key Metrics")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Current Price", f"${df['close'].iloc[0]:.2f}")
                with col2:
                    change = df['close'].iloc[0] - df['close'].iloc[1] if len(df) > 1 else 0
                    st.metric("Day Change", f"{change:.2f}")
                with col3:
                    st.metric("Volume", f"{df['volume'].iloc[0]:,}")
                with col4:
                    st.metric("Data Points", f"{len(df):,}")
            else:
                # Multiple symbols comparison
                st.subheader("Price Comparison")
                fig = px.line(df, x='date', y='close', color='symbol', title="Stock Price Comparison")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data found for the selected filters.")
    
    with col2:
        st.header("🔧 Pipeline Status")
        
        # Pipeline state
        pipeline_state = load_pipeline_state()
        if not pipeline_state.empty:
            st.subheader("Last Watermarks")
            for _, row in pipeline_state.iterrows():
                st.write(f"**{row['symbol']}**: {row['last_watermark']}")
        
        # Recent runs
        st.subheader("Recent Pipeline Runs")
        pipeline_runs = load_pipeline_runs()
        if not pipeline_runs.empty:
            for _, run in pipeline_runs.iterrows():
                status_emoji = "✅" if run['status'] == 'completed' else "❌" if run['status'] == 'failed' else "🔄"
                st.write(f"{status_emoji} {run['start_time']} - {run['status']}")
        
        # Data summary
        st.subheader("Data Summary")
        if not all_data.empty:
            st.metric("Total Records", f"{len(all_data):,}")
            st.metric("Symbols", f"{all_data['symbol'].nunique()}")
            st.metric("Date Range", f"{all_data['date'].min()} to {all_data['date'].max()}")
    
    # Footer
    st.markdown("---")
    st.markdown("💡 *Dashboard shows data from your financial data ingestion pipeline*")

if __name__ == "__main__":
    main()
