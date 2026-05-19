# =========================================================
# ULTRA FAST ADVANCE DOM SERVER.PY
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
    resources={
        r"/*": {
            "origins": "*"
        }
    }
)

# =========================================================
# SOCKET.IO
# =========================================================

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="gevent",
    ping_timeout=30,
    ping_interval=25
)

# =========================================================
# DHAN CONFIG
# =========================================================

ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"

CLIENT_ID = "YOUR_CLIENT_ID"

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

current_candle = None

market_depth = {
    "bids": [],
    "asks": []
}

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

    SCRIP_MASTER.dropna(
        subset=[COL_DISPLAY],
        inplace=True
    )

    SCRIP_MASTER["DISPLAY_UPPER"] = (
        SCRIP_MASTER[COL_DISPLAY]
        .astype(str)
        .str.upper()
    )

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

    return "ADVANCE DOM SERVER RUNNING"

# =========================================================
# SEARCH
# =========================================================

@app.route("/search")
@cross_origin()
def search_symbols():

    try:

        load_scrip_master()

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

        return jsonify({
            "s": "no_data"
        })

    except Exception as e:

        return jsonify({

            "s": "error",

            "errmsg": str(e)
        })

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
# DHAN WEBSOCKET
# =========================================================

def start_dhan_ws():

    print("🔥 Starting Dhan WS...")

    def on_message(ws, message):

        global market_depth

        try:

            packet_type = message[0]

            # =================================================
            # LTP
            # =================================================

            if packet_type == 2:

                ltp = round(
                    struct.unpack(
                        '<f',
                        message[8:12]
                    )[0],
                    2
                )

                candle = process_tick(ltp, 0)

                if candle:

                    socketio.emit(
                        "candle",
                        candle
                    )

            # =================================================
            # MARKET DEPTH
            # =================================================

            elif packet_type == 5:

                bids = []
                asks = []

                offset = 12

                # =============================================
                # BID LEVELS
                # =============================================

                for i in range(5):

                    qty = struct.unpack(
                        '<I',
                        message[offset:offset+4]
                    )[0]

                    price = round(
                        struct.unpack(
                            '<f',
                            message[offset+4:offset+8]
                        )[0],
                        2
                    )

                    orders = struct.unpack(
                        '<H',
                        message[offset+8:offset+10]
                    )[0]

                    bids.append({

                        "price": price,
                        "qty": qty,
                        "orders": orders,
                        "time": datetime.now().strftime("%H:%M:%S")
                    })

                    offset += 12

                # =============================================
                # ASK LEVELS
                # =============================================

                for i in range(5):

                    qty = struct.unpack(
                        '<I',
                        message[offset:offset+4]
                    )[0]

                    price = round(
                        struct.unpack(
                            '<f',
                            message[offset+4:offset+8]
                        )[0],
                        2
                    )

                    orders = struct.unpack(
                        '<H',
                        message[offset+8:offset+10]
                    )[0]

                    asks.append({

                        "price": price,
                        "qty": qty,
                        "orders": orders,
                        "time": datetime.now().strftime("%H:%M:%S")
                    })

                    offset += 12

                market_depth = {

                    "bids": bids,
                    "asks": asks
                }

                socketio.emit(
                    "dom_update",
                    market_depth
                )

        except Exception as e:

            print("❌ WS ERROR:", e)

    # =====================================================
    # OPEN
    # =====================================================

    def on_open(ws):

        print("✅ DHAN WS CONNECTED")

        subscribe = {

            "RequestCode": 21,

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

        print("📡 SUBSCRIBED")

    # =====================================================
    # ERROR
    # =====================================================

    def on_error(ws, error):

        print("❌ WS ERROR:", error)

    # =====================================================
    # CLOSE
    # =====================================================

    def on_close(ws, a, b):

        print("❌ WS CLOSED")

        time.sleep(5)

        start_ws_thread()

    # =====================================================
    # WS
    # =====================================================

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

    start_ws_thread()

    port = int(
        os.environ.get("PORT", 5000)
    )

    socketio.run(
        app,
        host="0.0.0.0",
        port=port
    )
