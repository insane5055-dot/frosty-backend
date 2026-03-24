import os
import pytz
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import pandas as pd
from datetime import datetime, timedelta
from flask_socketio import SocketIO
import websocket
import threading
import json
import time

app = Flask(__name__)
CORS(app)

# 🔥 SocketIO init
socketio = SocketIO(app, cors_allowed_origins="*")

# =========================
# 🔥 CANDLE ENGINE
# =========================
current_candle = None

def process_tick(price, volume):

    global current_candle

    ts = int(time.time())
    minute = ts // 60

    if current_candle is None:
        current_candle = {
            "minute": minute,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume
        }
        return None

    # SAME CANDLE
    if minute == current_candle["minute"]:
        current_candle["high"] = max(current_candle["high"], price)
        current_candle["low"] = min(current_candle["low"], price)
        current_candle["close"] = price
        current_candle["volume"] += volume
        return current_candle

    # NEW CANDLE
    finished = current_candle

    current_candle = {
        "minute": minute,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": volume
    }

    return finished

# ==================================================
# DHAN CONFIG
# ==================================================
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc0NDQ5OTEyLCJpYXQiOjE3NzQzNjM1MTIsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAxMzEwMzM0In0.NVh8ciau4nmSVWhb0bd3hEFcbwo40qBnHGa6EkQBPpjBK5hVexVMhxP8UkLGhBHHf5uK0ZZdTzeWmFr8mCKnqg"

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}


# =========================
# 🔥 DHAN WEBSOCKET
# =========================
import struct   # 👈 top pe

def start_dhan_ws():

    print("🔥 Starting Dhan WS...")

    def on_message(ws, message):

        try:
            # 🔥 Packet type check (VERY IMPORTANT)
            packet_type = message[0]

            if packet_type != 2:
                return   # ❌ ignore non-ticker packets

            # 🔥 Decode ticker packet
            ltp = round(struct.unpack('<f', message[8:12])[0], 2)
            ltt = struct.unpack('<i', message[12:16])[0]

            print("PRICE:", ltp, "TIME:", ltt)

            # 🔥 Candle engine
            candle = process_tick(ltp, 0)

            if candle:
                socketio.emit("candle", candle)

        except Exception as e:
            print("Decode Error:", e)

    def on_open(ws):
        print("✅ Connected")

        subscribe = {
            "RequestCode": 15,
            "InstrumentCount": 1,
            "InstrumentList": [
                {
                    "ExchangeSegment": "IDX_I",
                    "SecurityId": "13"
                }
            ]
        }

        time.sleep(1)
        ws.send(json.dumps(subscribe))

    def on_error(ws, error):
        print("❌ ERROR:", error)

    def on_close(ws, close_status_code, close_msg):
        print("❌ CLOSED:", close_status_code, close_msg)

    ws = websocket.WebSocketApp(
        f"wss://api-feed.dhan.co?version=2&token={ACCESS_TOKEN}&clientId=1101310334&authType=2",
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever(ping_interval=20)


# 🔥 Run WS in background
def start_ws_thread():
    print("🚀 Starting WS thread...")   # 👈 yaha add karna hai
    threading.Thread(target=start_dhan_ws, daemon=True).start()




# ==================================================
# LOAD SCRIP MASTER
# ==================================================
SCRIP_MASTER = pd.read_csv(
    "https://images.dhan.co/api-data/api-scrip-master-detailed.csv",
    low_memory=False
)
SCRIP_MASTER.columns = SCRIP_MASTER.columns.str.upper()

COL_DISPLAY    = next(c for c in SCRIP_MASTER.columns if "DISPLAY" in c)
COL_SECURITY   = next(c for c in SCRIP_MASTER.columns if "SECURITY" in c)
COL_EXCHANGE   = next(c for c in SCRIP_MASTER.columns if "EXCH" in c)
COL_INSTRUMENT = next(c for c in SCRIP_MASTER.columns if "INSTRUMENT" in c)

SCRIP_MASTER["DISPLAY_UPPER"] = SCRIP_MASTER[COL_DISPLAY].str.upper()

# ==================================================
# HOME
# ==================================================
@app.route("/")
def home():
    return "Backend running OK"

# ==================================================
# SEARCH SYMBOLS (FINAL)
# ==================================================

@app.route("/search")
def search_symbols():

    q = request.args.get("q", "").upper().strip()
    if not q:
        return jsonify([])

    q_upper = q.upper()

    # -----------------------------------
    # Index Aware Search
    # -----------------------------------
    if "BANKNIFTY" in q_upper:
        df = SCRIP_MASTER[SCRIP_MASTER["DISPLAY_UPPER"].str.startswith("BANKNIFTY")]
    elif "FINNIFTY" in q_upper:
        df = SCRIP_MASTER[SCRIP_MASTER["DISPLAY_UPPER"].str.startswith("FINNIFTY")]
    elif "NIFTY" in q_upper:
        df = SCRIP_MASTER[SCRIP_MASTER["DISPLAY_UPPER"].str.startswith("NIFTY")]
    else:
        df = SCRIP_MASTER.copy()

    # Fuzzy match
    df = df[df["DISPLAY_UPPER"].str.contains(q_upper, na=False)]

    # Exchange filter
    df = df[df[COL_EXCHANGE].isin(["IDX_I","NSE","NSE_FNO","BSE"])]

    # Remove garbage
    blocked_keywords = [
        "GIFT","EQUAL","PR ","TR ","LEVERAGE","INVERSE",
        "QUALITY","MOMENTUM","DEFENCE","SMALLCAP",
        "MIDCAP","EV "
    ]

    for word in blocked_keywords:
        df = df[~df["DISPLAY_UPPER"].str.contains(word, na=False)]

        # -----------------------------------
    # Priority Function
    # -----------------------------------
    def priority(row):
        name = row["DISPLAY_UPPER"]
        instr = row[COL_INSTRUMENT]
        exch = row[COL_EXCHANGE]

        # 1️⃣ Exact equity match
        if instr == "EQUITY" and name == q_upper:
            return 0

        # 2️⃣ Equity startswith
        if instr == "EQUITY" and name.startswith(q_upper):
            return 1

        # 3️⃣ Index match
        if "INDEX" in instr and name.startswith(q_upper):
            return 2

        # 4️⃣ NSE Equity general
        if instr == "EQUITY" and exch == "NSE":
            return 3

        # 5️⃣ Futures
        if instr in ["FUTIDX", "FUTSTK"]:
            return 4

        # 6️⃣ Options (last)
        if instr in ["OPTIDX", "OPTSTK"]:
            return 5

        return 6

    df["__P__"] = df.apply(priority, axis=1)
    df = df.sort_values(["__P__", "DISPLAY_UPPER"]).head(50)

    # -----------------------------------
    # Build Results
    # -----------------------------------
    results = []

    for _, r in df.iterrows():

        instr = r[COL_INSTRUMENT]

        if "INDEX" in instr:
            tv_type = "index"
        elif instr in ["FUTIDX","FUTSTK"]:
            tv_type = "futures"
        elif instr in ["OPTIDX","OPTSTK"]:
            tv_type = "option"
        else:
            tv_type = "stock"

        results.append({
            "symbol": r[COL_DISPLAY],
            "full_name": r[COL_EXCHANGE] + ":" + r[COL_DISPLAY],
            "description": r[COL_DISPLAY],
            "exchange": r[COL_EXCHANGE],
            "ticker": r[COL_EXCHANGE] + ":" + r[COL_DISPLAY],
            "type": tv_type
        })

    return jsonify(results)

# ==================================================
# RESOLVE SYMBOL (FIXED)
# ==================================================
@app.route("/resolve")
def resolve_symbol():

    symbol_raw = request.args.get("symbol", "").upper().strip()

    # 🔥 Split exchange from ticker (NSE:NTPC)
    if ":" in symbol_raw:
        exchange_param, symbol = symbol_raw.split(":", 1)
        symbol = symbol.replace(" ", "")
    else:
        exchange_param = ""
        symbol = symbol_raw.replace(" ", "")

    index_map = {
        "NIFTY": "NIFTY 50",
        "NIFTY50": "NIFTY 50",
        "BANKNIFTY": "NIFTY BANK",
        "NIFTYBANK": "NIFTY BANK",
        "FINNIFTY": "NIFTY FIN SERVICE"
    }

    row = pd.DataFrame()

    # ===============================
    # 1️⃣ INDEX EXACT MATCH
    # ===============================
    if symbol in index_map:
        display = index_map[symbol]
        row = SCRIP_MASTER[
            (SCRIP_MASTER[COL_DISPLAY].str.upper() == display) &
            (SCRIP_MASTER[COL_INSTRUMENT].str.contains("INDEX", na=False))
        ]

    # ===============================
    # 2️⃣ EXACT MATCH BY EXCHANGE
    # ===============================
    if row.empty:
        df_exact = SCRIP_MASTER[
            SCRIP_MASTER[COL_DISPLAY]
            .str.upper()
            .str.replace(" ", "") == symbol
        ]

        if exchange_param:
            df_exact = df_exact[df_exact[COL_EXCHANGE] == exchange_param]

        row = df_exact

    # ===============================
    # 3️⃣ CONTAINS MATCH (Fallback)
    # ===============================
    if row.empty:
        df_contains = SCRIP_MASTER[
            SCRIP_MASTER[COL_DISPLAY]
            .str.upper()
            .str.replace(" ", "")
            .str.contains(symbol, na=False)
        ]

        if exchange_param:
            df_contains = df_contains[df_contains[COL_EXCHANGE] == exchange_param]

        row = df_contains

    # ===============================
    # NOT FOUND
    # ===============================
    if row.empty:
        return jsonify({"error": "symbol not found"}), 404

    row = row.iloc[0]
    instrument = row[COL_INSTRUMENT]

    # ===============================
    # EXCHANGE SEGMENT
    # ===============================
    if "INDEX" in instrument:
        exchange_segment = "IDX_I"
        pricescale = 1
        supported_resolutions = ["1", "5", "15"]

    elif instrument in ["OPTIDX", "FUTIDX", "OPTSTK", "FUTSTK"]:
        exchange_segment = "NSE_FNO"
        pricescale = 100
        supported_resolutions = ["1", "5", "15"]

    else:
        exchange_segment = row[COL_EXCHANGE] + "_EQ"
        pricescale = 100
        supported_resolutions = ["1", "5", "15"]

    # ===============================
    # RETURN
    # ===============================
    return jsonify({
        "name": row[COL_DISPLAY],
        "ticker": row[COL_EXCHANGE] + ":" + row[COL_DISPLAY],
        "description": row[COL_DISPLAY],
        "type": instrument,
        "exchange": row[COL_EXCHANGE],
        "session": "0915-1530",
        "timezone": "Asia/Kolkata",
        "minmov": 1,
        "pricescale": pricescale,
        "has_intraday": True,
        "supported_resolutions": supported_resolutions,
        "security_id": str(row[COL_SECURITY]),
        "instrument": instrument,
        "exchange_segment": exchange_segment
    })

# ==================================================
# DHAN INTRADAY FETCH
# ==================================================
def get_intraday_ohlc(security_id, exchange, instrument, from_date, to_date):

    url = "https://api.dhan.co/v2/charts/intraday"

    payload = {
        "securityId": str(security_id),
        "exchangeSegment": exchange,
        "instrument": instrument,
        "interval": "1",
        "oi": False,
        "fromDate": from_date,
        "toDate": to_date
    }

    r = requests.post(url, headers=HEADERS, json=payload, timeout=10)

    if r.status_code != 200:
        return []

    res = r.json()

    if "open" not in res:
        return []

    data = []

    for i in range(len(res["open"])):
        ts = int(res["timestamp"][i])

        if len(str(ts)) == 13:
            ts //= 1000

        data.append({
            "time": ts,
            "open": res["open"][i],
            "high": res["high"][i],
            "low": res["low"][i],
            "close": res["close"][i],
            "volume": res.get("volume", [0])[i]
        })

    # ✅ loop ke baad sort
    data = sorted(data, key=lambda x: x["time"])

    return data

# ==================================================
# AGGREGATE
# ==================================================
def aggregate_candles(candles, step):

    out = []

    for i in range(0, len(candles), step):

        chunk = candles[i:i+step]

        if len(chunk) < step:
            continue

        out.append({
            "time": chunk[0]["time"],
            "open": chunk[0]["open"],
            "high": max(c["high"] for c in chunk),
            "low": min(c["low"] for c in chunk),
            "close": chunk[-1]["close"],
            "volume": sum(c["volume"] for c in chunk)
        })

    return out

# ==================================================
# HISTORY
# ==================================================

@app.route("/history")
def history():

    security_id = request.args.get("security_id")
    exchange    = request.args.get("exchange")
    instrument  = request.args.get("instrument")
    resolution  = request.args.get("resolution", "1")

    from_ts = int(request.args.get("from"))
    to_ts   = int(request.args.get("to"))

    ist = pytz.timezone("Asia/Kolkata")

    # 🔥 TradingView buffer
    from_dt = datetime.fromtimestamp(from_ts, tz=ist) - timedelta(days=5)
    to_dt   = datetime.fromtimestamp(to_ts, tz=ist)

    from_date = from_dt.strftime("%Y-%m-%d %H:%M:%S")
    to_date   = to_dt.strftime("%Y-%m-%d %H:%M:%S")

    candles_1m = get_intraday_ohlc(
        security_id,
        exchange,
        instrument,
        from_date,
        to_date
    )

    if not candles_1m:
        return jsonify({"s": "no_data"})

    # Resolution aggregation
    if resolution == "5":
        candles = aggregate_candles(candles_1m, 5)
    elif resolution == "15":
        candles = aggregate_candles(candles_1m, 15)
    else:
        candles = candles_1m

        candles = sorted(candles, key=lambda x: x["time"])

    if not candles:
        return jsonify({"s": "no_data"})

    return jsonify({
        "s": "ok",
        "t": [c["time"] for c in candles],
        "o": [c["open"] for c in candles],
        "h": [c["high"] for c in candles],
        "l": [c["low"] for c in candles],
        "c": [c["close"] for c in candles],
        "v": [c["volume"] for c in candles]
    })

# ==================================================
# START
# ==================================================
if __name__ == "__main__":
    start_ws_thread()
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port)