import yfinance as yf
import pyodbc
import pandas as pd

# ==============================
# CONFIG — CHANGE DATES MANUALLY
# ==============================
START_DATE = "2026-02-12"
END_DATE   = "2026-02-18"

BATCH_SIZE = 50

# ==============================
# SQL CONNECTION
# ==============================
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\SQLEXPRESS02;"
    "DATABASE=INVESTMENTS;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# ==============================
# MANUAL TICKER LIST
# ==============================
tickers = [
"AAPL","MSFT","AMZN","NVDA","GOOGL","GOOG","META","BRK.B","TSLA","UNH",
"JNJ","JPM","V","XOM","PG","MA","HD","LLY","CVX","MRK","ABBV","PEP","KO",
"AVGO","COST","BAC","TMO","WMT","CSCO","MCD","ABT","CRM","ACN","DHR","LIN",
"CMCSA","ORCL","ADBE","NKE","TXN","NEE","PM","AMD","NFLX","BMY","UPS","RTX",
"QCOM","HON","T","AMGN","LOW","INTC","IBM","INTU","SPGI","CAT","AMAT","GE",
"PLD","BA","GS","BLK","SYK","ISRG","MDT","ADP","ELV","GILD","DE","VRTX",
"DIS","MMC","CI","CB","C","REGN","SLB","PGR","SO","ZTS","DUK","LRCX","BDX",
"ITW","BSX","FI","ETN","AON","MO","COP","PANW","KLAC","CME","NOC","SNPS",
"WM","CDNS","CL","EOG","APD","MCO","TGT","FDX","SHW","HUM","PXD","USB",
"MCK","EMR","PSA","MMM","GD","ICE","ORLY","TJX","PNC","AZO","ROP","WFC",
"NSC","TFC","AEP","D","MSCI","CCI","WELL","SRE","AMP","MCHP","MPC","FTNT",
"EW","PCAR","PSX","F","GM","DXCM","HCA","DOW","MNST","OXY","CTAS","AJG",
"YUM","VLO","ECL","EXC","GIS","IDXX","ON","STZ","MRNA","HLT","ADM","BIIB",
"NXPI","A","DD","KMB","DLTR","AFL","CARR","GPN","WMB","CTVA","IQV","ROST",
"OTIS","CMG","JCI","XEL","AME","CHTR","TEL","PPG","CTSH","ES","MSI","KEYS",
"KDP","TROW","SYY","WEC","HPQ","VRSK","O","KHC","RSG","FAST","EA","DVN",
"ODFL","GWW","HAL","FANG","EQR","CBRE","DAL","BKR","APTV","ENPH","HSY",
"FIS","DG","MPWR","AVB","EFX","CDW","TRGP","SBAC","ROK","TSCO","DFS","COO",
"CPRT","WBA","LUV","ULTA","ANSS","MKC","ILMN","BALL","WBD","HIG","TDY",
"STT","ZBH","FITB","WY","NTRS","BAX","LYB","ALB","MTD","VTR","SBUX","LMT",
"SCHW","NOW","AMT","PFE","MDLZ","AXP","CNC","COF","CMI","ALL","KR","PAYX",
"LHX","TT","IFF","SWK","DHI","LEN","PWR","EL","NTAP","GLW","PAYC","TDG",
"IR","BR","RMD","NUE","CSGP","SEDG","CLX","MTB","EQT","URI","PKG","K",
"DTE","ED","POOL","BBY","PH","TECH","STE","IP","HPE","RF","KEY","WAB",
"VICI","FE","CFG","CHD","GPC","EXPD","LVS","VFC","MGM","CAH","HOLX",
"TTWO","XYL","WAT","TER","IEX","AKAM","VMC","MLM","STLD","WRB","ALGN",
"TYL","GRMN","MAA","DOV","BRO","RJF","ZBRA","INVH","ARE","MAS","J","ETSY",
"HWM","CE","MOH","EPAM","PTC","PEAK","L","AES","LNT","EVRG","NI","CMS",
"JBHT","HSIC","NVR","LW","SWKS","TXT","NDAQ","FTV","BG","PODD","TAP",
"UAL","AAL","NRG","RE","JNPR","WYNN","CHRW","SEE","CRL","QRVO","CTLT",
"CRWD","BWA","GNRC","FMC","RHI","DISH","NCLH","AAP","PARA","XRAY","BEN",
"RL","PENN","IVZ","CZR","ALLE","BBWI","HRL","CPB","SJM","HST","APA","MOS",
"CF","CMA","ZION","MKTX","RCL","CCL","LDOS","PHM","FOXA","FOX","NWSA",
"NWS","DPZ","SNA","EMN","JKHY","WRK","ATO","UDR","SPG","TMUS","VZ","MU",
"ADI","FCX","APH","MAR","ABNB","ZS","KVUE","GEV","DECK","FICO","IT",
"HUBB","AXON","VLTO","IRM","DOC","LII","WST","DDOG","SNOW","CEG","PCG",
"EIX","PPL","AEE","CNP","ETR","PEG","AWK","WTRG","SBNY","FRC","AMCR",
"LUMN","DVA","TPR","INCY","VTRS","PNR","TEAM","CBOE","AIZ","GL","TTD",
"BIO","FBHS","PNW","HAS","CPT","REG","ESS","KIM","FRT","BXP","SLG",
"TRMB","ROL","TFX","DGX","PLTR","AOS","LKQ","MTCH","VEEV","SPLK","FFIV",
"CG","NET","SOLV","MDB","GEHC","OKTA","HII","KMI","OKE","SMCI","VST",
"LH","GDDY","RBA","BLDR","GEN","FSLR","ERIE","TRV","MET","PRU","ACGL",
"SYF","COR","CINF","RVTY","NDSN","EME","ACM","IPG","OMC","WTW","AIG","UBER"
]

# Convert Yahoo special symbols
tickers = [t.replace(".", "-") for t in tickers]

print(f"Total tickers: {len(tickers)}")

# ==============================
# PROCESS IN BATCHES
# ==============================
for i in range(0, len(tickers), BATCH_SIZE):

    batch = tickers[i:i+BATCH_SIZE]
    print(f"Processing batch {i//BATCH_SIZE + 1}")

    data = yf.download(
        batch,
        start=START_DATE,
        end=END_DATE,
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False
    )

    for ticker in batch:

        if ticker not in data:
            continue

        df = data[ticker]

        if df.empty:
            continue

        df = df.reset_index()

        for _, row in df.iterrows():

            open_p  = None if pd.isna(row["Open"]) else round(float(row["Open"]), 2)
            close_p = None if pd.isna(row["Close"]) else round(float(row["Close"]), 2)
            high_p  = None if pd.isna(row["High"]) else round(float(row["High"]), 2)
            low_p   = None if pd.isna(row["Low"]) else round(float(row["Low"]), 2)
            volume  = None if pd.isna(row["Volume"]) else int(row["Volume"])

            cursor.execute("""
                INSERT INTO dbo.Stocks_History
                (ticker, date, open_price, close_price, high, low, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ticker,
            row["Date"].date(),
            open_p,
            close_p,
            high_p,
            low_p,
            volume
            )

# ==============================
# SAVE DATA
# ==============================
conn.commit()
cursor.close()
conn.close()

print("✅ Latest data loaded into dbo.Stocks_History")
