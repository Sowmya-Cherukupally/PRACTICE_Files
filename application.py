import streamlit as st
import urllib
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date, timedelta
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Stock Market Dashboard", page_icon="📈", layout="wide")

# ══════════════════════════════════════════════════════════════════════════════
#  CSS: CLEAN DARK THEME
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
    #MainMenu, footer, header { visibility: hidden; }
    .block-container { padding-top: 1.5rem; max-width: 1100px; margin: auto; }
    .lbl { font-size:.8rem; font-weight:700; color:#7b7f9e; text-transform:uppercase; margin-bottom:4px; margin-top:12px; }
    .stTextInput > div > div > input, .stSelectbox > div > div { 
        background:#1e2130 !important; border:1.5px solid #2e3354 !important; border-radius:10px !important; color:#fff !important; 
    }
    .kpi-row  { display:flex; gap:10px; margin:1.2rem 0; flex-wrap: nowrap; overflow-x: auto; }
    .kpi-card { flex:1; min-width:140px; background:#1a1d2e; border:1px solid #2a2d45; border-radius:12px; padding:15px; text-align:center; }
    .kpi-l { font-size:.65rem; color:#7b7f9e; font-weight:700; text-transform:uppercase; }
    .kpi-v { font-size:1.3rem; font-weight:800; color:#e8eaf6; margin-top:4px; }
    .up { color:#26a69a; font-weight:700; font-size:0.85rem; }
    .down { color:#ef5350; font-weight:700; font-size:0.85rem; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE CONNECTION
# ══════════════════════════════════════════════════════════════════════════════
_ODBC = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=.\\SQLEXPRESS02;DATABASE=INVESTMENTS;Trusted_Connection=yes;"
_URL = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(_ODBC)

@st.cache_resource
def get_engine():
    return create_engine(_URL, fast_executemany=True)

def load_data(ticker: str, n_rows: int = None, start: date = None, end: date = None) -> pd.DataFrame:
    t = ticker.replace("'", "''")
    if n_rows:
        query = f"SELECT TOP {int(n_rows)} CONVERT(VARCHAR(10), [date], 120) AS [date], [open_price] AS [Open], [high] AS [High], [low] AS [Low], [close_price] AS [Close], [volume] AS [Volume] FROM INVESTMENTS.dbo.Stocks_History WHERE ticker = '{t}' ORDER BY [date] DESC"
    else:
        query = f"SELECT CONVERT(VARCHAR(10), [date], 120) AS [date], [open_price] AS [Open], [high] AS [High], [low] AS [Low], [close_price] AS [Close], [volume] AS [Volume] FROM INVESTMENTS.dbo.Stocks_History WHERE ticker = '{t}' AND [date] >= '{start}' AND [date] <= '{end}' ORDER BY [date] ASC"
    
    with get_engine().connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    if df.empty: return df
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    return df

# ══════════════════════════════════════════════════════════════════════════════
#  USER INTERFACE
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("## 📈 Stock Market Dashboard")

st.markdown('<div class="lbl">Ticker Symbol</div>', unsafe_allow_html=True)
ticker_input = st.text_input("Ticker", placeholder="E.G. AAPL", label_visibility="collapsed").upper().strip()

st.markdown('<div class="lbl">Interval</div>', unsafe_allow_html=True)
interval_opts = ["5 Days", "1 Month", "3 Months", "6 Months", "Custom Range"]
sel_interval = st.selectbox("Interval", interval_opts, index=0, label_visibility="collapsed")

if sel_interval == "Custom Range":
    c1, c2 = st.columns(2)
    with c1: start_date = st.date_input("From", value=date.today() - timedelta(days=30))
    with c2: end_date = st.date_input("To", value=date.today())
else:
    start_date, end_date = None, None

st.markdown('<hr style="border-top:1px solid #2a2d45; margin:1.2rem 0">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  LOGIC & CHARTING
# ══════════════════════════════════════════════════════════════════════════════
if ticker_input:
    INTERVAL_MAP = {"5 Days": 5, "1 Month": 22, "3 Months": 66, "6 Months": 132}
    
    df = load_data(ticker_input, 
                   n_rows=None if sel_interval == "Custom Range" else INTERVAL_MAP[sel_interval],
                   start=start_date, end=end_date)

    if not df.empty:
        # KPI Cards for the VERY LATEST day
        lt, pv = df.iloc[-1], (df.iloc[-2] if len(df) > 1 else df.iloc[-1])
        chg = lt['Close'] - pv['Close']
        pct = (chg / pv['Close'] * 100) if pv['Close'] != 0 else 0
        arrow, cls = ("▲", "up") if chg >= 0 else ("▼", "down")

        st.markdown(f"""
        <div class="kpi-row">
            <div class="kpi-card"><div class="kpi-l">Open</div><div class="kpi-v">{lt['Open']:.2f}</div></div>
            <div class="kpi-card"><div class="kpi-l">High</div><div class="kpi-v">{lt['High']:.2f}</div></div>
            <div class="kpi-card"><div class="kpi-l">Low</div><div class="kpi-v">{lt['Low']:.2f}</div></div>
            <div class="kpi-card"><div class="kpi-l">Close</div><div class="kpi-v">{lt['Close']:.2f}</div>
                <div class="{cls}">{arrow} {abs(chg):.2f} ({abs(pct):.2f}%)</div></div>
            <div class="kpi-card"><div class="kpi-l">Volume</div><div class="kpi-v">{int(lt['Volume']):,}</div></div>
        </div>""", unsafe_allow_html=True)

        # CHARTING WITH DISTINCT COLORS
        x_dates = df.index.strftime("%Y-%m-%d").tolist()
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.08)
        
        # Open: Blue
        fig.add_trace(go.Scatter(x=x_dates, y=df["Open"], name="Open", mode="lines+markers", line=dict(color="#42a5f5")), row=1, col=1)
        # High: Green
        fig.add_trace(go.Scatter(x=x_dates, y=df["High"], name="High", mode="lines+markers", line=dict(color="#66bb6a")), row=1, col=1)
        # Low: Red
        fig.add_trace(go.Scatter(x=x_dates, y=df["Low"], name="Low", mode="lines+markers", line=dict(color="#ef5350")), row=1, col=1)
        # Close: Yellow (Brightest)
        fig.add_trace(go.Scatter(x=x_dates, y=df["Close"], name="Close", mode="lines+markers", line=dict(color="#ffd54f", width=3)), row=1, col=1)
        
        # Volume
        v_colors = ["#26a69a" if c >= o else "#ef5350" for c, o in zip(df["Close"], df["Open"])]
        fig.add_trace(go.Bar(x=x_dates, y=df["Volume"], name="Volume", marker_color=v_colors), row=2, col=1)

        fig.update_layout(paper_bgcolor="#13151f", plot_bgcolor="#13151f", height=600, font=dict(color="#c5cae9"),
                          margin=dict(l=0, r=0, t=10, b=0), hovermode="x unified", legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"))
        
        # CRITICAL FIX: Treat X-axis as 'category' so every day shows up as its own point
        fig.update_xaxes(type='category', gridcolor="#22253a", zeroline=False)
        fig.update_yaxes(gridcolor="#22253a", zeroline=False)
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(f"No database records found for {ticker_input}.")
else:
    st.info("💡 Enter a ticker symbol to load the 5-day daily chart.")
