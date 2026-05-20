// =========================================================
// GLOBALS
// =========================================================

let searchTimeout = null;

let lastSearchId = 0;

const BASE_URL =
    "https://frosty-backend-4mox.onrender.com";

// =========================================================
// SOCKET
// =========================================================

let socket = null;

let currentSubscriber = null;

// =========================================================
// DATAFEED
// =========================================================

const Datafeed = {

    // =====================================================
    // ON READY
    // =====================================================

    onReady: (callback) => {

        console.log("✅ Datafeed Ready");

        setTimeout(() => {

            callback({

                supported_resolutions: [

                    "1",
                    "5",
                    "15"
                ],

                exchanges: [

                    {
                        value: "NSE",
                        name: "NSE",
                        desc: "NSE"
                    },

                    {
                        value: "IDX_I",
                        name: "INDEX",
                        desc: "INDEX"
                    }
                ],

                symbols_types: [

                    {
                        name: "stock",
                        value: "stock"
                    },

                    {
                        name: "index",
                        value: "index"
                    }
                ],

                supports_search: true,

                supports_group_request: false,

                supports_marks: false,

                supports_timescale_marks: false,

                supports_time: true
            });

        }, 0);
    },

    // =====================================================
    // SEARCH SYMBOLS
    // =====================================================

    searchSymbols: (

        userInput,
        exchange,
        symbolType,
        onResultReadyCallback

    ) => {

        clearTimeout(searchTimeout);

        searchTimeout = setTimeout(async () => {

            try {

                const searchId = ++lastSearchId;

                console.log(
                    "🔍 Searching:",
                    userInput
                );

                const res = await fetch(

                    `${BASE_URL}/search?q=${encodeURIComponent(userInput)}`
                );

                const data = await res.json();

                if (searchId !== lastSearchId) {
                    return;
                }

                console.log(
                    "✅ Search Results:",
                    data
                );

                onResultReadyCallback(data);

            } catch (err) {

                console.error(
                    "❌ Search Error:",
                    err
                );

                onResultReadyCallback([]);
            }

        }, 300);
    },

    // =====================================================
    // RESOLVE SYMBOL
    // =====================================================

    resolveSymbol: async (

        symbolName,
        onSymbolResolvedCallback,
        onResolveErrorCallback

    ) => {

        try {

            console.log(
                "📌 Resolving:",
                symbolName
            );

            const res = await fetch(

                `${BASE_URL}/resolve?symbol=${encodeURIComponent(symbolName)}`
            );

            const data = await res.json();

            console.log(
                "✅ Resolve Response:",
                data
            );

            if (data.error) {

                onResolveErrorCallback(data.error);

                return;
            }

            const symbolInfo = {

                ticker:
                data.ticker,

                name:
                data.name,

                description:
                data.description,

                type:
                data.type,

                session:
                data.session,

                timezone:
                data.timezone,

                exchange:
                data.exchange,

                minmov:
                data.minmov,

                pricescale:
                data.pricescale,

                has_intraday:
                true,

                has_no_volume:
                false,

                has_daily:
                true,

                has_weekly_and_monthly:
                false,

                supported_resolutions:
                data.supported_resolutions,

                volume_precision:
                2,

                data_status:
                "streaming",

                visible_plots_set:
                "ohlcv",

                format:
                "price",

                security_id:
                data.security_id,

                instrument:
                data.instrument,

                exchange_segment:
                data.exchange_segment
            };

            console.log(
                "✅ Symbol Info:",
                symbolInfo
            );

            onSymbolResolvedCallback(symbolInfo);

        } catch (err) {

            console.error(
                "❌ Resolve Error:",
                err
            );

            onResolveErrorCallback(
                "Resolve failed"
            );
        }
    },

    // =====================================================
    // GET BARS
    // =====================================================

    getBars: async (

        symbolInfo,
        resolution,
        periodParams,
        onHistoryCallback,
        onErrorCallback

    ) => {

        try {

            console.log(
                "📚 Loading History..."
            );

            const {
                from,
                to,
                firstDataRequest
            } = periodParams;

            const url =

                `${BASE_URL}/history` +

                `?security_id=${symbolInfo.security_id}` +

                `&exchange=${symbolInfo.exchange_segment}` +

                `&instrument=${symbolInfo.instrument}` +

                `&resolution=${resolution}` +

                `&from=${from}` +

                `&to=${to}`;

            console.log(
                "🌐 HISTORY URL:",
                url
            );

            const response =
                await fetch(url);

            const data =
                await response.json();

            console.log(
                "📦 History Response:",
                data
            );

            if (
                !data ||
                data.s !== "ok" ||
                !data.t ||
                data.t.length === 0
            ) {

                console.log(
                    "⚠️ No history data"
                );

                onHistoryCallback(
                    [],
                    {
                        noData: true
                    }
                );

                return;
            }

            const bars = [];

            for (let i = 0; i < data.t.length; i++) {

                bars.push({

                    time:
                    data.t[i] * 1000,

                    open:
                    parseFloat(data.o[i]),

                    high:
                    parseFloat(data.h[i]),

                    low:
                    parseFloat(data.l[i]),

                    close:
                    parseFloat(data.c[i]),

                    volume:
                    parseFloat(data.v[i] || 0)
                });
            }

            console.log(
                "✅ Bars Loaded:",
                bars.length
            );

            onHistoryCallback(
                bars,
                {
                    noData: false
                }
            );

        } catch (err) {

            console.error(
                "❌ getBars Error:",
                err
            );

            onErrorCallback(err);
        }
    },

    // =====================================================
    // SUBSCRIBE BARS
    // =====================================================

    subscribeBars: (

        symbolInfo,
        resolution,
        onRealtimeCallback,
        subscriberUID,
        onResetCacheNeededCallback

    ) => {

        console.log(
            "📡 subscribeBars:",
            symbolInfo.name
        );

        currentSubscriber = {

            subscriberUID,
            resolution,
            symbolInfo,
            onRealtimeCallback
        };

        // =================================================
        // REUSE SOCKET
        // =================================================

        if (
            socket &&
            socket.connected
        ) {

            console.log(
                "♻️ Reusing Socket"
            );

            return;
        }

        // =================================================
        // CLEAN OLD SOCKET
        // =================================================

        if (socket) {

            socket.disconnect();

            socket = null;
        }

        // =================================================
        // CONNECT SOCKET
        // =================================================

        socket = io(BASE_URL, {

            transports: ["websocket"],

            reconnection: true,

            reconnectionAttempts: 9999,

            reconnectionDelay: 2000,

            timeout: 20000,

            forceNew: false
        });

        // =================================================
        // CONNECTED
        // =================================================

        socket.on("connect", () => {

            console.log(
                "✅ WebSocket Connected"
            );
        });

        // =================================================
        // DISCONNECT
        // =================================================

        socket.on("disconnect", (reason) => {

            console.log(
                "❌ Socket Disconnected:",
                reason
            );
        });

        // =================================================
        // ERROR
        // =================================================

        socket.on("connect_error", (err) => {

            console.log(
                "⚠️ WS Error:",
                err.message
            );
        });

        // =================================================
        // LIVE CANDLE
        // =================================================

        socket.on("candle", (candle) => {

            try {

                if (!currentSubscriber) {
                    return;
                }

                console.log(
                    "📊 Live Candle:",
                    candle
                );

                currentSubscriber
                    .onRealtimeCallback({

                        time:
                        candle.minute * 60 * 1000,

                        open:
                        parseFloat(candle.open),

                        high:
                        parseFloat(candle.high),

                        low:
                        parseFloat(candle.low),

                        close:
                        parseFloat(candle.close),

                        volume:
                        parseFloat(candle.volume || 0)
                    });

            } catch (err) {

                console.log(
                    "❌ Candle Error:",
                    err
                );
            }
        });
    },

    // =====================================================
    // UNSUBSCRIBE
    // =====================================================

    unsubscribeBars: (subscriberUID) => {

        console.log(
            "🛑 unsubscribeBars:",
            subscriberUID
        );

        currentSubscriber = null;
    }
};

// =========================================================
// EXPORT
// =========================================================

export default Datafeed;
