# =========================================================
# FINAL UPDATED SERVER.PY
# =========================================================

import os
import pytz
import requests
import pandas as pd
import websocket
import threading
import json
import time
import struct

from datetime import datetime, timedelta

from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO

# =========================================================
# FLASK APP
# =========================================================

app = Flask(__name__)

CORS(
    app,
    resources={r"/*": {"origins": "*"}},
    supports_credentials=True
)

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading"
)

# =========================================================
# AFTER REQUEST (IMPORTANT FOR CORS)
# =========================================================

@app.after_request
def after_request(response):

    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "*")
    response.headers.add("Access-Control-Allow-Methods", "*")

    return response

# =========================================================
# DHAN CONFIG
# =========================================================

ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}

# =========================================================
# LOAD SCRIP MASTER
# =========================================================

SCRIP_MASTER = None

COL_DISPLAY = None
COL_SECURITY = None
COL_EXCHANGE = None
COL_INSTRUMENT = None

def load_scrip_master():

    global SCRIP_MASTER
    global COL_DISPLAY
    global COL_SECURITY
    global COL_EXCHANGE
    global COL_INSTRUMENT

    if SCRIP_MASTER is not None:
        return

    print("🔥 Loading Scrip Master...")

    SCRIP_MASTER = pd.read_csv(
        "https://images.dhan.co/api-data/api-scrip-master-detailed.csv",
        low_memory=False
    )

    SCRIP_MASTER.columns = SCRIP_MASTER.columns.str.upper()

    COL_DISPLAY = next(c for c in SCRIP_MASTER.columns if "DISPLAY" in c)
    COL_SECURITY = next(c for c in SCRIP_MASTER.columns if "SECURITY" in c)
    COL_EXCHANGE = next(c for c in SCRIP_MASTER.columns if "EXCH" in c)
    COL_INSTRUMENT = next(c for c in SCRIP_MASTER.columns if "INSTRUMENT" in c)

    SCRIP_MASTER["DISPLAY_UPPER"] = (
        SCRIP_MASTER[COL_DISPLAY]
        .astype(str)
        .str.upper()
    )

    print("✅ Scrip Master Loaded")

# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():
    return "Backend running OK"

# =========================================================
# SEARCH
# =========================================================

@app.route("/search")
def search_symbols():

    try:

        load_scrip_master()

        q = request.args.get("q", "").upper().strip()

        print("SEARCH:", q)

        if not q:
            return jsonify([])

        df = SCRIP_MASTER.copy()

        df = df[
            df["DISPLAY_UPPER"]
            .str.contains(q, na=False)
        ]

        df = df[
            df[COL_EXCHANGE]
            .isin(["NSE", "BSE", "NSE_FNO", "IDX_I"])
        ]

        results = []

        for _, r in df.head(50).iterrows():

            instr = str(r[COL_INSTRUMENT])

            if "INDEX" in instr:
                tv_type = "index"

            elif instr in ["FUTIDX", "FUTSTK"]:
                tv_type = "futures"

            elif instr in ["OPTIDX", "OPTSTK"]:
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

    except Exception as e:

        print("SEARCH ERROR:", str(e))

        return jsonify({
            "error": str(e)
        }), 500

# =========================================================
# RESOLVE
# =========================================================

@app.route("/resolve")
def resolve_symbol():

    try:

        load_scrip_master()

        symbol_raw = request.args.get("symbol", "").upper().strip()

        print("RESOLVE:", symbol_raw)

        if ":" in symbol_raw:
            exchange_param, symbol = symbol_raw.split(":", 1)
        else:
            exchange_param = ""
            symbol = symbol_raw

        symbol = symbol.replace(" ", "")

        df = SCRIP_MASTER.copy()

        df["MATCH"] = (
            df[COL_DISPLAY]
            .astype(str)
            .str.upper()
            .str.replace(" ", "")
        )

        row = df[df["MATCH"] == symbol]

        if exchange_param:
            row = row[row[COL_EXCHANGE] == exchange_param]

        if row.empty:
            return jsonify({
                "error": "symbol not found"
            }), 404

        row = row.iloc[0]

        instrument = str(row[COL_INSTRUMENT])

        # =====================================================
        # EXCHANGE SEGMENT
        # =====================================================

        if "INDEX" in instrument:

            exchange_segment = "IDX_I"
            pricescale = 1

        elif instrument in ["OPTIDX", "FUTIDX", "OPTSTK", "FUTSTK"]:

            exchange_segment = "NSE_FNO"
            pricescale = 100

        else:

            exchange_segment = "NSE_EQ"
            pricescale = 100

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

            "has_daily": True,

            "has_weekly_and_monthly": True,

            "supported_resolutions": ["1", "5", "15"],

            "data_status": "streaming",

            "security_id": str(row[COL_SECURITY]),

            "instrument": instrument,

            "exchange_segment": exchange_segment

        })

    except Exception as e:

        print("RESOLVE ERROR:", str(e))

        return jsonify({
            "error": str(e)
        }), 500

# =========================================================
# INTRADAY FETCH
# =========================================================

def get_intraday_ohlc(
    security_id,
    exchange,
    instrument,
    from_date,
    to_date
):

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

    r = requests.post(
        url,
        headers=HEADERS,
        json=payload,
        timeout=15
    )

    if r.status_code != 200:
        print("DHAN ERROR:", r.text)
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

    data = sorted(data, key=lambda x: x["time"])

    return data

# =========================================================
# AGGREGATE
# =========================================================

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

# =========================================================
# HISTORY
# =========================================================

@app.route("/history")
def history():

    try:

        security_id = request.args.get("security_id")
        exchange = request.args.get("exchange")
        instrument = request.args.get("instrument")
        resolution = request.args.get("resolution", "1")

        from_ts = int(request.args.get("from"))
        to_ts = int(request.args.get("to"))

        ist = pytz.timezone("Asia/Kolkata")

        from_dt = datetime.fromtimestamp(
            from_ts,
            tz=ist
        ) - timedelta(days=5)

        to_dt = datetime.fromtimestamp(
            to_ts,
            tz=ist
        )

        from_date = from_dt.strftime("%Y-%m-%d %H:%M:%S")
        to_date = to_dt.strftime("%Y-%m-%d %H:%M:%S")

        candles_1m = get_intraday_ohlc(
            security_id,
            exchange,
            instrument,
            from_date,
            to_date
        )

        if not candles_1m:
            return jsonify({"s": "no_data"})

        if resolution == "5":
            candles = aggregate_candles(candles_1m, 5)

        elif resolution == "15":
            candles = aggregate_candles(candles_1m, 15)

        else:
            candles = candles_1m

        return jsonify({

            "s": "ok",

            "t": [c["time"] for c in candles],

            "o": [c["open"] for c in candles],

            "h": [c["high"] for c in candles],

            "l": [c["low"] for c in candles],

            "c": [c["close"] for c in candles],

            "v": [c["volume"] for c in candles]

        })

    except Exception as e:

        print("HISTORY ERROR:", str(e))

        return jsonify({
            "s": "error",
            "errmsg": str(e)
        })

# =========================================================
# WEBSOCKET
# =========================================================

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

    if minute == current_candle["minute"]:

        current_candle["high"] = max(
            current_candle["high"],
            price
        )

        current_candle["low"] = min(
            current_candle["low"],
            price
        )

        current_candle["close"] = price

        current_candle["volume"] += volume

        return current_candle

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

def start_dhan_ws():

    print("🔥 Starting Dhan WS...")

    def on_message(ws, message):

        try:

            packet_type = message[0]

            if packet_type != 2:
                return

            ltp = round(
                struct.unpack('<f', message[8:12])[0],
                2
            )

            candle = process_tick(ltp, 0)

            if candle:
                socketio.emit("candle", candle)

        except Exception as e:

            print("WS DECODE ERROR:", e)

    def on_open(ws):

        print("✅ WS Connected")

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

        print("❌ WS ERROR:", error)

    def on_close(ws, a, b):

        print("❌ WS CLOSED")

    ws = websocket.WebSocketApp(

        f"wss://api-feed.dhan.co?version=2&token={ACCESS_TOKEN}&clientId=1101310334&authType=2",

        on_message=on_message,

        on_open=on_open,

        on_error=on_error,

        on_close=on_close
    )

    ws.run_forever(ping_interval=20)

# =========================================================
# START WS THREAD
# =========================================================

def start_ws_thread():

    threading.Thread(
        target=start_dhan_ws,
        daemon=True
    ).start()

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    start_ws_thread()

    port = int(os.environ.get("PORT", 5000))

    socketio.run(
        app,
        host="0.0.0.0",
        port=port,
        allow_unsafe_werkzeug=True
    )
