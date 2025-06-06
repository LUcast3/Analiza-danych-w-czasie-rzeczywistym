import streamlit as st
import pandas as pd
import time
import altair as alt

st.set_page_config(page_title="Stock Alerts Dashboard", layout="wide")
st.title("📈 Stock Anomaly Alerts Dashboard")

csv_file = "alerts_log.csv"
REFRESH_INTERVAL = 5

while True:
    try:
        df = pd.read_csv(csv_file)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp", ascending=False)
        
        symbols = df["symbol"].unique()
        selected_symbol = st.selectbox("🪙 Wybierz symbol", options=["Wszystkie"] + list(symbols))
        
        if selected_symbol != "Wszystkie":
            df = df[df["symbol"] == selected_symbol]

        st.subheader("🔔 Ostatnie alerty")
        st.dataframe(df.head(20), use_container_width=True)

        st.subheader("📊 Z-score wolumenu (ostatnie 50 alertów)")
        st.line_chart(df.head(50).set_index("timestamp")["zscore"])

        # 📈 Dodatkowe wykresy:

        # 1. Histogram typów alertów
        st.subheader("📋 Liczba alertów wg typu")
        alert_count = df["alert_type"].value_counts().reset_index()
        alert_count.columns = ["alert_type", "count"]
        st.bar_chart(alert_count.set_index("alert_type"))

        # 2. Cena zamknięcia w czasie
        st.subheader("💰 Cena zamknięcia (ostatnie 50)")
        st.line_chart(df.head(50).set_index("timestamp")["close"])

        # 3. Wolumen w czasie
        st.subheader("📦 Wolumen (ostatnie 50)")
        st.line_chart(df.head(50).set_index("timestamp")["volume"])

        # 4. Średni z-score w czasie dla każdego symbolu
        st.subheader("📈 Średni Z-score w czasie (7-dniowe średnie)")

        # Zaokrąglenie timestampów do dnia
        df["day"] = df["timestamp"].dt.floor("D")

        # Grupowanie po dniu i symbolu
        zscore_trend = df.groupby(["day", "symbol"])["zscore"].mean().reset_index()

        line = alt.Chart(zscore_trend).mark_line().encode(
            x="day:T",
            y="zscore:Q",
            color="symbol:N",
            tooltip=["day:T", "symbol:N", "zscore:Q"]
        ).properties(width=800, height=400)

        st.altair_chart(line, use_container_width=True)

    except Exception as e:
        st.warning(f"Błąd podczas wczytywania danych: {e}")
    
    time.sleep(REFRESH_INTERVAL)
    st.rerun()
