# =========================================================
# ULTRA FAST FROSTY SERVER.PY
# REALTIME TICK EMA + SOCKET.IO
# =========================================================

from gevent import monkey
monkey.patch_all()

import os
import json
import time
import struct
import threading
import requests
import pandas as pd
import pytz
import websocket

from datetime import datetime, timedelta

from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
from flask_socketio import SocketIO

# =========================================================
# FLASK APP
# =========================================================

app = Flask(__name__)

# =========================================================
# CORS
# =========================================================

CORS(
    app,
    resources={r"/*": {"origins": "*"}}
)

@app.after_request
def after_request(response):

    response.headers["Access-Control-Allow-Origin"] = "*"

    response.headers["Access-Control-Allow-Headers"] = "*"

    response.headers["Access-Control-Allow-Methods"] = "*"

    return response

# =========================================================
# SOCKET.IO
# =========================================================

socketio = SocketIO(

    app,

    cors_allowed_origins="*",

    async_mode="gevent",

    ping_timeout=30,

    ping_interval=25,

    transports=["websocket"],

    logger=False,

    engineio_logger=False
)

# =========================================================
# DHAN CONFIG
# =========================================================

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NzIyNjE2LCJpYXQiOjE3Nzg2MzYyMTYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAxMzEwMzM0In0.5zmxbhxu1jzWLtAQNtD2TiZ26h8HaksG4IpC61NSREj4lwyNHeVmViDCGdngTCU9UVAHtgtPgolF99r2M_idbQ"

CLIENT_ID = "1101310334"

HEADERS = {

    "Accept": "application/json",

    "Content-Type": "application/json",

    "access-token": ACCESS_TOKEN
}

# =========================================================
# GLOBALS
# =========================================================

SCRIP_MASTER = None

SEARCH_CACHE = []

COL_DISPLAY = None
COL_SECURITY = None
COL_EXCHANGE = None
COL_INSTRUMENT = None

# =========================================================
# EMA GLOBALS
# =========================================================

EMA_PERIOD = 20

EMA_MULTIPLIER = 2 / (EMA_PERIOD + 1)

tick_ema = None

# =========================================================
# LIVE CANDLE
# =========================================================

current_candle = None

# =========================================================
# LOAD SCRIP MASTER
# =========================================================

def load_scrip_master():

    global SCRIP_MASTER
    global SEARCH_CACHE

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

    SCRIP_MASTER.columns = (
        SCRIP_MASTER.columns.str.upper()
    )

    COL_DISPLAY = next(
        c for c in SCRIP_MASTER.columns
        if "DISPLAY" in c
    )

    COL_SECURITY = next(
        c for c in SCRIP_MASTER.columns
        if "SECURITY" in c
    )

    COL_EXCHANGE = next(
        c for c in SCRIP_MASTER.columns
        if "EXCH" in c
    )

    COL_INSTRUMENT = next(
        c for c in SCRIP_MASTER.columns
        if "INSTRUMENT" in c
    )

    # =====================================================
    # CLEAN
    # =====================================================

    SCRIP_MASTER.dropna(
        subset=[COL_DISPLAY],
        inplace=True
    )

    # =====================================================
    # PREPROCESS
    # =====================================================

    SCRIP_MASTER["DISPLAY_UPPER"] = (

        SCRIP_MASTER[COL_DISPLAY]

        .astype(str)

        .str.upper()
    )

    # =====================================================
    # SEARCH CACHE
    # =====================================================

    for _, row in SCRIP_MASTER.iterrows():

        exchange = str(row[COL_EXCHANGE])

        if exchange not in [
            "NSE",
            "BSE",
            "NSE_FNO",
            "IDX_I"
        ]:
            continue

        SEARCH_CACHE.append({

            "display_upper":
            str(row[COL_DISPLAY]).upper(),

            "symbol":
            row[COL_DISPLAY],

            "exchange":
            exchange,

            "instrument":
            row[COL_INSTRUMENT]
        })

    print("⚡ Search cache ready:", len(SEARCH_CACHE))

    print("✅ Scrip Master Loaded")

# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():

    return jsonify({

        "status": "running",

        "message": "Frosty Backend Running"
    })

# =========================================================
# SEARCH
# =========================================================

@app.route("/search")
@cross_origin()
def search_symbols():

    try:

        q = request.args.get(
            "q",
            ""
        ).upper().strip()

        if not q:
            return jsonify([])

        exact = []
        starts = []
        contains = []

        for item in SEARCH_CACHE:

            name = item["display_upper"]

            if name == q:

                exact.append(item)

            elif name.startswith(q):

                starts.append(item)

            elif q in name:

                contains.append(item)

        final = (

            exact[:5] +

            starts[:10] +

            contains[:10]
        )

        results = []

        for item in final:

            instr = str(item["instrument"])

            if "INDEX" in instr:

                tv_type = "index"

            elif instr in [
                "FUTIDX",
                "FUTSTK"
            ]:

                tv_type = "futures"

            elif instr in [
                "OPTIDX",
                "OPTSTK"
            ]:

                tv_type = "option"

            else:

                tv_type = "stock"

            results.append({

                "symbol":
                item["symbol"],

                "ticker":
                f"{item['exchange']}:{item['symbol']}",

                "full_name":
                f"{item['exchange']}:{item['symbol']}",

                "description":
                item["symbol"],

                "exchange":
                item["exchange"],

                "type":
                tv_type
            })

        return jsonify(results)

    except Exception as e:

        print("❌ SEARCH ERROR:", e)

        return jsonify([])

# =========================================================
# RESOLVE
# =========================================================

@app.route("/resolve")
@cross_origin()
def resolve_symbol():

    try:

        load_scrip_master()

        symbol_raw = request.args.get(
            "symbol",
            ""
        ).upper().strip()

        print("📌 RESOLVE:", symbol_raw)

        if ":" in symbol_raw:

            exchange_param, symbol = (
                symbol_raw.split(":", 1)
            )

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

            row = row[
                row[COL_EXCHANGE] == exchange_param
            ]

        if row.empty:

            return jsonify({
                "error": "symbol not found"
            }), 404

        row = row.iloc[0]

        instrument = str(row[COL_INSTRUMENT])

        # =================================================
        # EXCHANGE SEGMENT
        # =================================================

        if "INDEX" in instrument:

            exchange_segment = "IDX_I"

            pricescale = 1

        elif instrument in [
            "OPTIDX",
            "FUTIDX",
            "OPTSTK",
            "FUTSTK"
        ]:

            exchange_segment = "NSE_FNO"

            pricescale = 100

        else:

            if row[COL_EXCHANGE] == "BSE":

                exchange_segment = "BSE_EQ"

            else:

                exchange_segment = "NSE_EQ"

            pricescale = 100

        return jsonify({

            "name":
            row[COL_DISPLAY],

            "ticker":
            f"{row[COL_EXCHANGE]}:{row[COL_DISPLAY]}",

            "description":
            row[COL_DISPLAY],

            "type":
            instrument,

            "exchange":
            row[COL_EXCHANGE],

            "session":
            "0915-1530",

            "timezone":
            "Asia/Kolkata",

            "minmov":
            1,

            "pricescale":
            pricescale,

            "has_intraday":
            True,

            "has_daily":
            True,

            "has_weekly_and_monthly":
            True,

            "supported_resolutions":
            ["1", "5", "15"],

            "data_status":
            "streaming",

            "security_id":
            str(row[COL_SECURITY]),

            "instrument":
            instrument,

            "exchange_segment":
            exchange_segment
        })

    except Exception as e:

        print("❌ RESOLVE ERROR:", str(e))

        return jsonify({
            "error": str(e)
        }), 500

# =========================================================
# HISTORY
# =========================================================

@app.route("/history")
@cross_origin()
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

        from_date = from_dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        to_date = to_dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

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

            timeout=20
        )

        if r.status_code != 200:

            print("❌ DHAN ERROR:", r.text)

            return jsonify({
                "s": "no_data"
            })

        res = r.json()

        if "open" not in res:

            return jsonify({
                "s": "no_data"
            })

        candles = []

        for i in range(len(res["open"])):

            ts = int(res["timestamp"][i])

            if len(str(ts)) == 13:
                ts //= 1000

            candles.append({

                "time": ts,

                "open": res["open"][i],

                "high": res["high"][i],

                "low": res["low"][i],

                "close": res["close"][i],

                "volume": res.get("volume", [0])[i]
            })

        candles = sorted(
            candles,
            key=lambda x: x["time"]
        )

        return jsonify({

            "s": "ok",

            "t":
            [c["time"] for c in candles],

            "o":
            [c["open"] for c in candles],

            "h":
            [c["high"] for c in candles],

            "l":
            [c["low"] for c in candles],

            "c":
            [c["close"] for c in candles],

            "v":
            [c["volume"] for c in candles]
        })

    except Exception as e:

        print("❌ HISTORY ERROR:", str(e))

        return jsonify({

            "s": "error",

            "errmsg": str(e)
        })

# =========================================================
# TICK EMA
# =========================================================

def calculate_tick_ema(price):

    global tick_ema

    if tick_ema is None:

        tick_ema = price

    else:

        tick_ema = (

            (price - tick_ema)

            * EMA_MULTIPLIER

        ) + tick_ema

    return round(tick_ema, 2)

# =========================================================
# PROCESS TICK
# =========================================================

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

# =========================================================
# SOCKET EVENTS
# =========================================================

@socketio.on("connect")
def handle_connect():

    print("🟢 Frontend Connected")

@socketio.on("disconnect")
def handle_disconnect():

    print("🔴 Frontend Disconnected")

# =========================================================
# DHAN WEBSOCKET
# =========================================================

def start_dhan_ws():

    print("🔥 Starting Dhan WS...")

    def on_message(ws, message):

        try:

            packet_type = message[0]

            if packet_type != 2:
                return

            # =================================================
            # LTP
            # =================================================

            ltp = round(

                struct.unpack(
                    '<f',
                    message[8:12]
                )[0],

                2
            )

            # =================================================
            # EMA
            # =================================================

            ema_value = calculate_tick_ema(ltp)

            socketio.emit(

                "tick_ema",

                {

                    "time": int(time.time()),

                    "price": ltp,

                    "ema": ema_value
                }
            )

            # =================================================
            # LIVE CANDLE
            # =================================================

            candle = process_tick(ltp, 0)

            if candle:

                socketio.emit(
                    "candle",
                    candle
                )

        except Exception as e:

            print("❌ WS DECODE ERROR:", e)

    def on_open(ws):

        print("✅ Dhan WS Connected")

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

        print("❌ DHAN WS ERROR:", error)

    def on_close(ws, a, b):

        print("❌ DHAN WS CLOSED")

        time.sleep(5)

        start_ws_thread()

    ws = websocket.WebSocketApp(

        f"wss://api-feed.dhan.co?version=2&token={ACCESS_TOKEN}&clientId={CLIENT_ID}&authType=2",

        on_message=on_message,

        on_open=on_open,

        on_error=on_error,

        on_close=on_close
    )

    ws.run_forever(

        ping_interval=20,

        ping_timeout=10
    )

# =========================================================
# START THREAD
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

    load_scrip_master()

    start_ws_thread()

    port = int(
        os.environ.get("PORT", 5000)
    )

    print(f"🚀 Server Running On Port {port}")

    socketio.run(

        app,

        host="0.0.0.0",

        port=port,

        debug=False,

        use_reloader=False
    )
