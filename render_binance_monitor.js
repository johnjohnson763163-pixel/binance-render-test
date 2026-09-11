import http from "http";
import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";


// ===============================
// SUPABASE
// ===============================

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
);

console.log("SUPABASE CHECK:", {
    url: !!process.env.SUPABASE_URL,
    key: !!process.env.SUPABASE_SERVICE_KEY
});


// ===============================
// TELEGRAM
// ===============================

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT = process.env.TELEGRAM_CHAT;

async function sendTelegram(text) {

    console.log("TELEGRAM MESSAGE:", text);

    if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT) {
        console.log("TELEGRAM CONFIG ERROR: TELEGRAM_TOKEN or TELEGRAM_CHAT missing");
        return false;
    }

    try {
        const response = await fetch(
            `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
            {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    chat_id: TELEGRAM_CHAT,
                    text
                })
            }
        );

        const result = await response.json().catch(() => ({}));

        if (!response.ok || result.ok === false) {
            console.log("TELEGRAM SEND ERROR:", result.description || response.statusText);
            return false;
        }

        return true;

    } catch (e) {
        console.log("Telegram error:", e.message);
        return false;
    }
}


// ===============================
// LEVEL STATE
// ===============================

let activeLevels = [];
let monitoredSymbols = new Set();
let websocket = null;
let websocketSymbolsKey = "";


// ===============================
// GET LEVELS FROM SUPABASE
// ===============================

async function getLevels() {

    const { data, error } = await supabase
        .from("levels")
        .select("*")
        .eq("triggered", false)
        .eq("active", true);

    if (error) {
        console.log("SUPABASE READ ERROR:", error.message);
        return [];
    }

    return data || [];
}


// ===============================
// REFRESH LEVELS
// ===============================

async function refreshLevels() {

    const levels = await getLevels();

    activeLevels = levels;

    const symbols = new Set();

    for (const level of levels) {
        const symbol = String(level.symbol || "").trim().toUpperCase();

        if (symbol) {
            symbols.add(symbol);
        }
    }

    const nextSymbolsKey = [...symbols].sort().join(",");

    monitoredSymbols = symbols;

    console.log(
        "LEVELS:",
        levels.length,
        "SYMBOLS:",
        [...symbols].join(", ") || "none"
    );

    if (nextSymbolsKey !== websocketSymbolsKey) {
        websocketSymbolsKey = nextSymbolsKey;
        restartBinanceWebSocket();
    }
}


// ===============================
// TIME FORMAT
// ===============================

function localTime() {

    return new Date().toLocaleString(
        "ru-RU",
        {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit"
        }
    );
}


// ===============================
// CHECK ALERTS
// ===============================

async function checkLevels(symbol, price) {

    const normalizedSymbol = String(symbol || "").toUpperCase();

    // Work only with levels that came from Supabase.
    const levels = activeLevels.filter(
        level =>
            String(level.symbol || "").toUpperCase() === normalizedSymbol &&
            level.active !== false &&
            level.triggered !== true
    );

    for (const level of levels) {

        const levelPrice = Number(level.price);

        if (!Number.isFinite(levelPrice)) {
            continue;
        }

        let hit = false;

        if (level.condition === "above" && price >= levelPrice) {
            hit = true;
        }

        if (level.condition === "below" && price <= levelPrice) {
            hit = true;
        }

        if (!hit) {
            continue;
        }


        // ===============================
        // COOLDOWN
        // ===============================

        const cooldown =
            Number(level.cooldown_seconds || 60) * 1000;

        if (level.last_trigger_time) {

            const last =
                new Date(level.last_trigger_time).getTime();

            if (
                Number.isFinite(last) &&
                Date.now() - last < cooldown
            ) {
                continue;
            }
        }


        // ===============================
        // TELEGRAM MESSAGE
        // ===============================

        const title =
            level.alert_name ||
            "Сигнальный уровень";

        const instrument =
            level.instrument ||
            "B-F";

        const message =
`🔔 ${title} · ${normalizedSymbol} · ${instrument}

Цена: ${price}

Уровень: ${levelPrice}

Время: ${localTime()}`;


        const sent = await sendTelegram(message);

        if (!sent) {
            continue;
        }


        // ===============================
        // SAVE TRIGGER STATE
        // ===============================

        const nowIso = new Date().toISOString();

        const { error } = await supabase
            .from("levels")
            .update({
                last_trigger_price: price,
                last_trigger_time: nowIso,
                updated_at: nowIso
            })
            .eq("id", level.id);

        if (error) {
            console.log(
                "SUPABASE UPDATE ERROR:",
                error.message
            );
        } else {
            // Update local state immediately so the next Binance ticks
            // cannot resend the same level during the cooldown.
            level.last_trigger_price = price;
            level.last_trigger_time = nowIso;
            level.updated_at = nowIso;
        }
    }
}


// ===============================
// BINANCE WEBSOCKET
// ===============================

function restartBinanceWebSocket() {

    if (websocket) {
        try {
            websocket.removeAllListeners();
            websocket.close();
        } catch (e) {
            console.log("WS CLOSE ERROR:", e.message);
        }

        websocket = null;
    }

    if (!monitoredSymbols.size) {
        console.log("BINANCE: no active levels, websocket not started");
        return;
    }

    const streams = [...monitoredSymbols]
        .map(symbol => `${symbol.toLowerCase()}@aggTrade`)
        .join("/");

    const url =
        `wss://fstream.binance.com/market/stream?streams=${streams}`;

    console.log(
        "BINANCE CONNECTING:",
        [...monitoredSymbols].join(", ")
    );

    const ws = new WebSocket(url);

    websocket = ws;

    ws.on(
        "open",
        () => {
            console.log("BINANCE CONNECTED");
        }
    );

    ws.on(
        "message",
        async data => {

            try {

                const msg = JSON.parse(data.toString());

                if (!msg || !msg.data) {
                    return;
                }

                const symbol =
                    String(msg.data.s || "").toUpperCase();

                const price =
                    Number(msg.data.p);

                if (!symbol || !Number.isFinite(price)) {
                    return;
                }

                console.log(
                    symbol,
                    price
                );

                await checkLevels(
                    symbol,
                    price
                );

            } catch (e) {

                console.log(
                    "BINANCE MESSAGE ERROR:",
                    e.message
                );

            }
        }
    );

    ws.on(
        "error",
        e => {
            console.log(
                "WS ERROR:",
                e.message
            );
        }
    );

    ws.on(
        "close",
        () => {

            console.log("BINANCE DISCONNECTED");

            if (websocket === ws) {
                websocket = null;
            }

            // Reconnect only if active levels still exist.
            setTimeout(
                () => {

                    if (
                        monitoredSymbols.size &&
                        websocket === null
                    ) {
                        restartBinanceWebSocket();
                    }

                },
                3000
            );
        }
    );
}


// ===============================
// HTTP SERVER
// ===============================

const server =
http.createServer(
    async (req, res) => {

        res.setHeader(
            "Content-Type",
            "application/json; charset=utf-8"
        );


        const url =
            new URL(
                req.url,
                "http://localhost"
            );


        // ===============================
        // GET LEVELS
        // ===============================

        if (
            req.method === "GET" &&
            url.pathname === "/api/levels"
        ) {

            const levels = await getLevels();

            res.end(
                JSON.stringify(levels)
            );

            return;
        }


        // ===============================
        // POST LEVEL
        // ===============================

        if (
            req.method === "POST" &&
            url.pathname === "/api/levels"
        ) {

            let body = "";

            req.on(
                "data",
                chunk => {
                    body += chunk;
                }
            );

            req.on(
                "end",
                async () => {

                    let data;

                    try {

                        data = JSON.parse(body);

                    } catch (e) {

                        console.log(
                            "JSON ERROR:",
                            e.message
                        );

                        res.statusCode = 400;

                        res.end(
                            JSON.stringify({
                                ok: false,
                                error: "Invalid JSON"
                            })
                        );

                        return;
                    }


                    if (
                        !data ||
                        typeof data !== "object" ||
                        Array.isArray(data)
                    ) {

                        res.statusCode = 400;

                        res.end(
                            JSON.stringify({
                                ok: false,
                                error: "Invalid JSON object"
                            })
                        );

                        return;
                    }


                    const symbol =
                        String(data.symbol || "")
                            .trim()
                            .toUpperCase();

                    const price =
                        Number(data.price);

                    if (
                        !symbol ||
                        !Number.isFinite(price)
                    ) {

                        res.statusCode = 400;

                        res.end(
                            JSON.stringify({
                                ok: false,
                                error: "Invalid symbol or price"
                            })
                        );

                        return;
                    }


                    const level = {

                        symbol,

                        price,

                        type: data.type,

                        condition:
                            data.condition ||
                            data.type,

                        active: true,

                        alert_name:
                            data.alert_name ||
                            "Сигнальный уровень",

                        exchange:
                            data.exchange ||
                            "Binance",

                        market:
                            data.market ||
                            "Futures",

                        instrument:
                            data.instrument ||
                            "B-F",

                        alert_type:
                            data.alert_type ||
                            "level",

                        triggered: false

                    };


                    const {
                        data: created,
                        error
                    } = await supabase
                        .from("levels")
                        .insert(level)
                        .select()
                        .single();


                    if (error) {

                        res.statusCode = 500;

                        res.end(
                            JSON.stringify({
                                ok: false,
                                error: error.message
                            })
                        );

                        return;
                    }


                    console.log(
                        "LEVEL CREATED",
                        created
                    );


                    // Immediately refresh the monitored symbol set.
                    await refreshLevels();


                    res.end(
                        JSON.stringify({
                            ok: true,
                            level: created
                        })
                    );

                }
            );

            return;
        }


        res.end(
            JSON.stringify({
                ok: true,
                service: "Render Binance Monitor GS"
            })
        );
    }
);


// ===============================
// START SERVER
// ===============================

server.listen(
    process.env.PORT || 10000,
    () => {

        console.log(
            "HTTP server started"
        );

        // Initial read of levels created from the graph.
        refreshLevels();

        // Keep Supabase levels synchronized with the monitor.
        setInterval(
            refreshLevels,
            5000
        );

    }
);
