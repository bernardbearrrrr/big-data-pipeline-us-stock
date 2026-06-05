import streamlit as st
import pandas as pd
import json
import os
import plotly.graph_objects as go
import plotly.express as px
from datetime import timedelta

# 1. Konfigurasi Halaman Dasar (Tanpa Emoji, Bersih)
st.set_page_config(page_title="US Stock TradingView", layout="wide", initial_sidebar_state="collapsed")

# 2. Path Data
DATA_DIR = os.getenv("DASHBOARD_DIR", "/app/dashboard_data")
HISTORY_FILE = os.path.join(DATA_DIR, "history.jsonl")
BATCH_FILE = os.path.join(DATA_DIR, "batch_result.json") 

# 3. KUSTOMISASI CSS TINGKAT DEWA (Anti-Kedip & Watchlist Sempurna)
st.markdown("""
    <style>
    [data-testid="stAppViewContainer"] [data-stale="true"], 
    [data-testid="stFragment"] {
        opacity: 1 !important;
        transition: none !important;
        filter: none !important;
    }
    [data-testid="stStatusWidget"] {
        display: none !important;
        visibility: hidden !important;
    }
    /* ================================================================= */

    /* Mengunci tinggi aplikasi agar grafik utama tidak bergerak sama sekali */
    .stApp {
        height: 100vh;
        overflow: hidden;
    }
    
    /* Mengatur kolom kedua (sidebar kanan) agar dapat di-scroll mandiri */
    div[data-testid="column"]:nth-of-type(2) {
        max-height: 88vh;
        overflow-y: auto !important;
        padding-right: 12px;
        scrollbar-width: thin;
        scrollbar-color: #2a2e39 #1e222d;
    }
    div[data-testid="column"]:nth-of-type(2)::-webkit-scrollbar { width: 6px; }
    div[data-testid="column"]:nth-of-type(2)::-webkit-scrollbar-track { background: #1e222d; }
    div[data-testid="column"]:nth-of-type(2)::-webkit-scrollbar-thumb { background: #2a2e39; border-radius: 3px; }

    /* Desain Metrik Key Stats */
    .metric-container { 
        background-color: #1c2030; 
        padding: 14px; 
        border-radius: 6px; 
        margin-bottom: 12px; 
        border: 1px solid #2a2e39;
    }
    
    /* Header Watchlist Rata Kiri & Kanan Sempurna */
    .wl-header {
        display: grid;
        grid-template-columns: 25% 25% 25% 25%;
        padding: 0px 5px;
        color: #848e9c;
        font-size: 12px;
        font-weight: bold;
        border-bottom: 1px solid #2a2e39;
        margin-bottom: 5px;
        padding-bottom: 8px;
    }
    .wl-header span:nth-child(1) { text-align: left; }
    .wl-header span:nth-child(2) { text-align: right; }
    .wl-header span:nth-child(3) { text-align: right; }
    .wl-header span:nth-child(4) { text-align: right; }

    /* =================================================================
       HACK CSS: MENYULAP TOMBOL JADI GRID RATA KIRI KANAN
       ================================================================= */
    div[data-testid="stButton"] button {
        background-color: transparent !important;
        border: none !important;
        border-bottom: 1px solid #1e222d !important;
        border-radius: 0px !important;
        padding: 10px 0px !important; /* Hapus padding kiri kanan agar bisa mentok */
        margin-bottom: 2px !important;
        width: 100% !important;
        box-shadow: none !important;
    }
    div[data-testid="stButton"] button:hover {
        background-color: #2a2e39 !important;
        border-radius: 4px !important;
    }

    /* INI KUNCI UTAMANYA: Memaksa container teks di dalam tombol untuk melar 100% */
    div[data-testid="stButton"] button div[data-testid="stMarkdownContainer"] {
        width: 100% !important;
    }

    div[data-testid="stButton"] button p {
        display: grid !important;
        grid-template-columns: 25% 25% 25% 25% !important;
        width: 100% !important;
        margin: 0 !important;
        align-items: center !important;
    }
    
    /* Mencegah teks terlipat ke bawah (Anti Mepet/Wrap) */
    div[data-testid="stButton"] button p > * {
        white-space: nowrap !important;
        display: block !important;
    }
    
    /* Alignment Spesifik untuk elemen di dalam tombol */
    div[data-testid="stButton"] button p strong { text-align: left !important; color: #ffffff !important; font-size: 14px !important; }
    div[data-testid="stButton"] button p em { text-align: right !important; font-style: normal !important; color: #d1d4dc !important; font-size: 14px !important; font-weight: bold !important; }
    div[data-testid="stButton"] button p span:nth-of-type(1) { text-align: right !important; font-size: 13px !important; font-weight: bold !important; }
    div[data-testid="stButton"] button p span:nth-of-type(2) { text-align: right !important; font-size: 13px !important; font-weight: bold !important; }
    /* ================================================================= */
    
    </style>
""", unsafe_allow_html=True)

# 4. State Management (Mengingat Saham Terakhir yang Diklik)
if "current_ticker" not in st.session_state:
    st.session_state.current_ticker = "AAPL"

# 5. Fungsi Load Data
def load_streaming_data():
    if os.path.exists(HISTORY_FILE):
        try:
            records = [json.loads(line) for line in open(HISTORY_FILE, "r", encoding="utf-8") if line.strip()]
            if records:
                df = pd.DataFrame(records)
                df['Date'] = pd.to_datetime(df['Date'])
                return df.sort_values("Date")
        except: pass
    return pd.DataFrame()

def load_batch_data():
    if os.path.exists(BATCH_FILE):
        try:
            with open(BATCH_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass
    return {}

# 6. Pengecekan Kesiapan Data
df_static = load_streaming_data()

if df_static.empty:
    st.title("Menunggu Aliran Data...")
    st.info("Spark Streaming sedang menyiapkan data. Pastikan producer.py dan streaming_job.py sudah berjalan.")
    st.stop()

# 7. Menu Tab Navigasi Utama
tab_live, tab_market, tab_batch = st.tabs(["Live TradingView", "Market Overview", "Batch Analytics"])

# ==========================================
# TAB 1: LIVE TRADINGVIEW (Fragment Anti-Kedip)
# ==========================================
with tab_live:
    @st.fragment(run_every="3s")
    def render_live_dashboard():
        df = load_streaming_data()
        if df.empty: return

        # Layout Kiri 75% (Tetap), Kanan 25% (Scrollable)
        col_chart, col_side = st.columns([3, 1], gap="large")
        
        ticker_list = sorted(df["Ticker"].dropna().astype(str).unique())
        if st.session_state.current_ticker not in ticker_list:
            st.session_state.current_ticker = ticker_list[0] if ticker_list else "AAPL"
            
        with col_chart:
            # Dropdown Search
            selected_ticker = st.selectbox("Symbol Search", ticker_list, index=ticker_list.index(st.session_state.current_ticker), label_visibility="collapsed")
            st.session_state.current_ticker = selected_ticker
            
            ticker_df = df[df["Ticker"] == selected_ticker].copy()
            if ticker_df.empty: return

            latest_date = ticker_df['Date'].max()
            latest_data = ticker_df.iloc[-1]
            company_name = ticker_df["Company_Name"].iloc[0]
            
            # Hitung Kenaikan
            if len(ticker_df) > 1:
                prev_close = ticker_df.iloc[-2]['Close']
                change_val = latest_data['Close'] - prev_close
                change_pct = (change_val / prev_close) * 100
                color = "#26a69a" if change_val >= 0 else "#ef5350"
                sign = "+" if change_val >= 0 else ""
            else:
                change_val = change_pct = 0
                color = "gray"
                sign = ""

            # Header Nama dan Harga
            st.markdown(f"### {company_name} ({selected_ticker})")
            st.markdown(f"<h1 style='color:{color}; margin-top:-15px;'>${latest_data['Close']:.2f} <span style='font-size: 20px;'>{sign}{change_val:.2f} ({sign}{change_pct:.2f}%)</span></h1>", unsafe_allow_html=True)
            
            # Filter Tanggal (Radio Button Bawah Judul)
            filter_options = ["1D", "3D", "1W", "1M", "3M", "1Y", "ALL"]
            selected_range = st.radio("Time Range", filter_options, horizontal=True, label_visibility="collapsed")
            
            start_date = ticker_df['Date'].min()
            if selected_range == "1D": start_date = latest_date
            elif selected_range == "3D": start_date = latest_date - timedelta(days=3)
            elif selected_range == "1W": start_date = latest_date - timedelta(days=7)
            elif selected_range == "1M": start_date = latest_date - timedelta(days=30)
            elif selected_range == "3M": start_date = latest_date - timedelta(days=90)
            elif selected_range == "1Y": start_date = latest_date - timedelta(days=365)
            
            filtered_df = ticker_df[ticker_df['Date'] >= start_date]

            # Grafik Candlestick
            fig = go.Figure(data=[go.Candlestick(
                x=filtered_df['Date'], open=filtered_df['Open'], high=filtered_df['High'],
                low=filtered_df['Low'], close=filtered_df['Close'],
                increasing_line_color='#26a69a', decreasing_line_color='#ef5350'
            )])
            fig.update_layout(
                height=550, 
                margin=dict(l=0, r=0, t=10, b=0), 
                xaxis_rangeslider_visible=False, 
                template='plotly_dark', 
                paper_bgcolor='rgba(0,0,0,0)', 
                plot_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_side:
            # --- BAGIAN 1: WATCHLIST GRID RAPI ---
            st.markdown("<h4 style='margin-bottom: 5px; color: white;'>Watchlist</h4>", unsafe_allow_html=True)
            
            # Header Watchlist (Sekarang persis ukurannya 25% x 4)
            st.markdown("""
            <div class="wl-header">
                <span>Symbol</span>
                <span>Last</span>
                <span>Chg</span>
                <span>Chg%</span>
            </div>
            """, unsafe_allow_html=True)
            
            latest_market_df = df[df['Date'] == latest_date]
            top_watchlist = latest_market_df.sort_values("Volume", ascending=False).head(5)
            
            for _, row in top_watchlist.iterrows():
                tckr = row['Ticker']
                hist_tckr = df[df['Ticker'] == tckr]
                
                if len(hist_tckr) > 1:
                    p_close = hist_tckr.iloc[-2]['Close']
                    c_val = row['Close'] - p_close
                    c_pct = (c_val / p_close) * 100
                    if c_val >= 0:
                        color_tag, val_sign = "green", "+"
                    else:
                        color_tag, val_sign = "red", ""
                    pct_str = f"{c_pct:.2f}%"
                else:
                    color_tag, val_sign, c_val, pct_str = "gray", "", 0, "0.00%"

                # INI DIA MAGISNYA: Streamlit akan membungkus ini dalam <p> tag,
                # dan CSS kita akan menyulapnya jadi GRID!
                btn_label = f"**{tckr}** *{row['Close']:.2f}* :{color_tag}[{val_sign}{c_val:.2f}] :{color_tag}[{val_sign}{pct_str}]"
                
                if st.button(btn_label, key=f"wl_{tckr}", use_container_width=True):
                    st.session_state.current_ticker = tckr
                    st.rerun()
            
            st.markdown("<div style='margin-bottom: 10px;'></div>", unsafe_allow_html=True)

            # --- BAGIAN 2: KEY STATS & GRAFIK (SCROLLABLE AREA) ---
            with st.container(height=380):
                st.markdown("<h4 style='color: white;'>Key Stats</h4>", unsafe_allow_html=True)
                st.markdown(f"""
                <div class="metric-container">
                    <p style='margin:0; color:#848e9c; font-size:12px'>Volume Terakhir</p>
                    <h4 style='margin:0;'>{latest_data['Volume'] / 1_000_000:,.2f} M</h4>
                </div>
                <div class="metric-container">
                    <p style='margin:0; color:#848e9c; font-size:12px'>Rentang Harian (Low - High)</p>
                    <h4 style='margin:0;'>${latest_data['Low']:.2f} - ${latest_data['High']:.2f}</h4>
                </div>
                <div class="metric-container">
                    <p style='margin:0; color:#848e9c; font-size:12px'>Sektor Industri</p>
                    <h5 style='margin:0; color:white;'>{ticker_df["Sector"].iloc[0]}</h5>
                </div>
                """, unsafe_allow_html=True)

                # Indikator Teknikal MA
                st.markdown("<br><h5 style='color: white;'>Technical (MA5 & MA10)</h5>", unsafe_allow_html=True)
                st.markdown("<p style='font-size: 11px; color: gray; margin-top: -10px;'>Momentum tren jangka pendek.</p>", unsafe_allow_html=True)
                
                ma_df = ticker_df.copy()
                ma_df['MA5'] = ma_df['Close'].rolling(window=5).mean()
                ma_df['MA10'] = ma_df['Close'].rolling(window=10).mean()
                
                tech_fig = go.Figure()
                tech_fig.add_trace(go.Scatter(x=ma_df['Date'], y=ma_df['Close'], name='Close', line=dict(color='white', width=1)))
                tech_fig.add_trace(go.Scatter(x=ma_df['Date'], y=ma_df['MA5'], name='MA5', line=dict(color='#2962ff', width=1.5)))
                tech_fig.add_trace(go.Scatter(x=ma_df['Date'], y=ma_df['MA10'], name='MA10', line=dict(color='#ef5350', width=1.5)))
                
                tech_fig.update_layout(height=150, margin=dict(l=0, r=0, t=0, b=0), xaxis_visible=False, yaxis_visible=False, template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', showlegend=False)
                st.plotly_chart(tech_fig, use_container_width=True)

                # Volume Momentum
                st.markdown("<br><h5 style='color: white;'>Volume Momentum (14 Days)</h5>", unsafe_allow_html=True)
                st.markdown("<p style='font-size: 11px; color: gray; margin-top: -10px;'>Aktivitas transaksi pasar terbaru.</p>", unsafe_allow_html=True)
                
                vol_fig = go.Figure(go.Bar(x=ticker_df['Date'].tail(14), y=ticker_df['Volume'].tail(14), marker_color='#82a0fc'))
                vol_fig.update_layout(height=120, margin=dict(l=0, r=0, t=0, b=0), xaxis_visible=False, yaxis_visible=False, template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(vol_fig, use_container_width=True)

                # All-Time Trend
                st.markdown("<br><h5 style='color: white;'>Historical Trend</h5>", unsafe_allow_html=True)
                st.markdown("<p style='font-size: 11px; color: gray; margin-top: -10px;'>Pertumbuhan sejak awal data masuk.</p>", unsafe_allow_html=True)
                
                mini_fig = go.Figure(go.Scatter(x=ticker_df['Date'], y=ticker_df['Close'], fill='tozeroy', line=dict(color='#26a69a', width=2)))
                mini_fig.update_layout(height=120, margin=dict(l=0, r=0, t=0, b=0), xaxis_visible=False, yaxis_visible=False, template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(mini_fig, use_container_width=True)

    # Eksekusi Render
    render_live_dashboard()

# ==========================================
# TAB 2 & 3: MARKET OVERVIEW & BATCH (TETAP SAMA)
# ==========================================
with tab_market:
    st.header("Keseluruhan Pasar (Live Market Pulse)")
    st.markdown("Ikhtisar pergerakan seluruh saham berdasarkan data perdagangan hari terakhir yang masuk dari Kafka.")
    
    # Mengambil data khusus untuk hari terakhir (real-time state)
    if not df_static.empty:
        latest_date_m = df_static['Date'].max()
        latest_market = df_static[df_static['Date'] == latest_date_m].copy()
        
        # Kalkulasi % Kenaikan Harian (Close terhadap Open)
        latest_market['Daily_Change_Pct'] = ((latest_market['Close'] - latest_market['Open']) / latest_market['Open']) * 100
        
        # --- 1. LIVE MARKET PULSE (Metrik Utama) ---
        total_vol = latest_market['Volume'].sum()
        up_stocks = len(latest_market[latest_market['Daily_Change_Pct'] > 0])
        down_stocks = len(latest_market[latest_market['Daily_Change_Pct'] < 0])
        top_gainer = latest_market.loc[latest_market['Daily_Change_Pct'].idxmax()]
        
        # Menampilkan 3 Kartu Metrik di atas
        m1, m2, m3 = st.columns(3)
        with m1:
            st.markdown(f"""
            <div class="metric-container" style="text-align:center;">
                <p style='margin:0; color:#848e9c; font-size:13px'>Total Volume Hari Ini</p>
                <h3 style='margin:0; color:#2962ff;'>{total_vol / 1_000_000:,.0f} Juta</h3>
            </div>
            """, unsafe_allow_html=True)
        with m2:
            st.markdown(f"""
            <div class="metric-container" style="text-align:center;">
                <p style='margin:0; color:#848e9c; font-size:13px'>Top Gainer Today</p>
                <h3 style='margin:0; color:#26a69a;'>{top_gainer['Ticker']} (+{top_gainer['Daily_Change_Pct']:.2f}%)</h3>
            </div>
            """, unsafe_allow_html=True)
        with m3:
            sentiment_color = "#26a69a" if up_stocks >= down_stocks else "#ef5350"
            st.markdown(f"""
            <div class="metric-container" style="text-align:center;">
                <p style='margin:0; color:#848e9c; font-size:13px'>Market Sentiment (Up / Down)</p>
                <h3 style='margin:0; color:{sentiment_color};'>{up_stocks} / {down_stocks} Saham</h3>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # --- 2. PRICE HEATMAP (TREEMAP) ---
        st.subheader("Price Heatmap")
        st.markdown("<p style='font-size: 13px; color: gray; margin-top:-10px;'>Ukuran kotak berdasarkan Volume transaksi, warna berdasarkan Kenaikan/Penurunan Harian (%).</p>", unsafe_allow_html=True)
        
        # Membangun grafik Treemap yang mewah
        fig_tree = px.treemap(
            latest_market, 
            path=[px.Constant("US Market"), 'Sector', 'Ticker'], 
            values='Volume',
            color='Daily_Change_Pct',
            color_continuous_scale='RdYlGn', # Skala warna Merah-Kuning-Hijau
            color_continuous_midpoint=0,
            custom_data=['Company_Name', 'Daily_Change_Pct', 'Close']
        )
        # Kustomisasi tooltip saat kursor diarahkan ke kotak saham
        fig_tree.update_traces(
            hovertemplate="<b>%{label}</b><br>Company: %{customdata[0]}<br>Close Price: $%{customdata[2]:.2f}<br>Daily Change: %{customdata[1]:.2f}%<br>Volume: %{value}<extra></extra>"
        )
        fig_tree.update_layout(margin=dict(t=20, l=10, r=10, b=10), template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', height=450)
        st.plotly_chart(fig_tree, use_container_width=True)

        st.markdown("---")

        # --- 3. DATA LAMA YANG DIPERTAHANKAN (BOTTOM SECTION) ---
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Top 10 Volume")
            top_vol = df_static.groupby(["Ticker", "Company_Name"])["Volume"].sum().reset_index().sort_values("Volume", ascending=False).head(10)
            top_vol["Volume (M)"] = top_vol["Volume"].apply(lambda x: f"${x / 1_000_000:,.2f} M")
            st.dataframe(top_vol.set_index("Ticker"), use_container_width=True)
        with c2:
            st.subheader("Distribusi Sektor")
            st.bar_chart(df_static["Sector"].value_counts(), color="#2962ff")
            
    else:
        st.warning("Menunggu aliran data market dari Kafka...")

with tab_batch:
    st.header("Historical Batch Analytics")
    st.markdown("Hasil pengolahan Apache Spark Batch yang mengkalkulasi jutaan baris sejarah dari CSV secara langsung tanpa melalui streaming.")
    
    batch_data = load_batch_data()
    
    if not batch_data:
        st.warning("Data Batch belum tersedia. Jalankan command di terminal:\ndocker exec -it bdp-spark-master /opt/spark/bin/spark-submit /opt/project/jobs/batch_analysis.py")
    else:
        sector_df = pd.DataFrame(batch_data.get("sector_analysis", []))
        gainers_df = pd.DataFrame(batch_data.get("top_gainers", []))
        volume_df = pd.DataFrame(batch_data.get("top_volume", []))
        yearly_df = pd.DataFrame(batch_data.get("yearly_performance", []))

        bc1, bc2 = st.columns(2)
        with bc1:
            st.subheader("Risiko (Volatilitas) Sektor")
            if not sector_df.empty and "Avg_Volatility" in sector_df.columns:
                fig_vol = px.bar(sector_df.sort_values("Avg_Volatility", ascending=False), x="Sector", y="Avg_Volatility", color="Avg_Volatility", color_continuous_scale="Reds")
                fig_vol.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig_vol, use_container_width=True)

        with bc2:
            st.subheader("Pangsa Volume per Sektor")
            if not sector_df.empty and "Total_Volume" in sector_df.columns:
                fig_pie = px.pie(sector_df, values="Total_Volume", names="Sector", hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
                fig_pie.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig_pie, use_container_width=True)

        st.markdown("---")

        bc3, bc4 = st.columns(2)
        with bc3:
            st.subheader("Tren Harga Tahunan Sektor")
            if not yearly_df.empty:
                fig_yearly = px.line(yearly_df, x="Year", y="Avg_Close_Price", color="Sector", markers=True)
                fig_yearly.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig_yearly, use_container_width=True)

        with bc4:
            st.subheader("Top Gainers Harian (Historis)")
            if not gainers_df.empty:
                fig_gains = px.bar(gainers_df.sort_values("Avg_Daily_Gain", ascending=True), y="Ticker", x="Avg_Daily_Gain", color="Avg_Daily_Gain", orientation='h', color_continuous_scale="Viridis", hover_data=["Company_Name", "Sector"])
                fig_gains.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig_gains, use_container_width=True)