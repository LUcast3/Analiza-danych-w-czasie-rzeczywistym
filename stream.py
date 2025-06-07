import json
import time
import glob
import os
import pandas as pd
from kafka import KafkaProducer

SERVER = "localhost:9092"  
TOPIC = "prices"       
SLEEP_INTERVAL = 0.0001

MAX_TIME = 300 
def load_data():
    files = glob.glob("*_minute.csv")
    if not files:
        raise FileNotFoundError("Nie znaleziono plików *_minute.csv w katalogu.")
    dfs = []
    for fn in files:
        ticker = os.path.basename(fn).split("_minute")[0]
        df_tmp = pd.read_csv(fn, parse_dates=["date"])
        df_tmp.rename(columns={"date": "DateTime"}, inplace=True)
        df_tmp["Symbol"] = ticker
        dfs.append(df_tmp)
    df = pd.concat(dfs, ignore_index=True)
    df.sort_values(["DateTime"], inplace=True,ascending=True)
    df.reset_index(drop=True, inplace=True)
    return df

if __name__ == "__main__":
    try:
        df = load_data()
    except Exception as e:
        print(f"Blad przy wczytywaniu danych: {e}")
        exit(1)
        
    # Inicjalizacja producenta Kafka
    producer = KafkaProducer(
        bootstrap_servers=[SERVER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    print("📤 Rozpoczynam wysyłanie ticków do topicu 'prices'...")
    start_time = time.time()

    try:
        for _, row in df.iterrows():
            if time.time() - start_time > MAX_TIME:
                print(f"\n⏱ Osiągnięto limit czasu {MAX_TIME} sekund. Zatrzymywanie producenta...")
                break
            
            message = {
                "timestamp": row["DateTime"].isoformat(),
                "open":      row["open"],
                "high":      row["high"],
                "low":       row["low"],
                "close":     row["close"],
                "volume":    int(row["volume"]),
                "Symbol":    row["Symbol"]
            }
            producer.send(TOPIC, value=message)
            time.sleep(SLEEP_INTERVAL)
    except KeyboardInterrupt:
        print("\n🛑 Zatrzymano producenta.")
    finally:
        producer.close()
        print("Producent zamknięty.")
