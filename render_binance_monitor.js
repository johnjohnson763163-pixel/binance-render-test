import http from “http”; import WebSocket from “ws”; import {
createClient } from “@supabase/supabase-js”;

// =============================== // SUPABASE //
===============================

const supabase = createClient( process.env.SUPABASE_URL,
process.env.SUPABASE_SERVICE_KEY );

console.log(“SUPABASE CHECK:”, { url: !!process.env.SUPABASE_URL, key:
!!process.env.SUPABASE_SERVICE_KEY });

// =============================== // TELEGRAM //
===============================

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN; const TELEGRAM_CHAT =
process.env.TELEGRAM_CHAT;

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

// =============================== // ALERT STATE //
===============================

let activeAlerts = []; let monitoredSymbols = new Set(); let websocket =
null; let websocketSymbolsKey = ““; const priceHistory = new Map();
const PRICE_HISTORY_MS = 65 * 60 * 1000; const MARKET_REFRESH_MS = 5000;
let marketRefreshRunning = false;

// =============================== // HELPERS //
===============================

function normalizeSymbol(value) { return String(value ||
““).trim().toUpperCase(); }

function nowIso() { return new Date().toISOString(); }

function numeric(value) { const n = Number(value); return
Number.isFinite(n) ? n : null; }

function getNestedItems(node) { return Array.isArray(node?.items) ?
node.items : []; }

function evaluateOperator(left, operator, right) { switch (operator) {
case “>”: return left > right; case “>=”: return left >= right; case
“<”: return left < right; case “<=”: return left <= right; case “=”:
case “==”: return left === right; case “!=”: return left !== right;
default: return false; } }

function previousPrice(symbol, windowMs) { const history =
priceHistory.get(symbol) || []; const cutoff = Date.now() - windowMs;

    for (let i = history.length - 1; i >= 0; i--) {
        if (history[i].time <= cutoff) {
            return history[i].price;
        }
    }

    return null;

}

function rememberPrice(symbol, price) { const history =
priceHistory.get(symbol) || []; const now = Date.now();

    history.push({ time: now, price });

    const cutoff = now - PRICE_HISTORY_MS;
    while (history.length && history[0].time < cutoff) {
        history.shift();
    }

    priceHistory.set(symbol, history);

}

function crossesLevel(symbol, price, levelPrice) { const history =
priceHistory.get(symbol) || []; if (!history.length) return false;

    const previous = history[history.length - 1].price;

    return (
        (previous < levelPrice && price >= levelPrice) ||
        (previous > levelPrice && price <= levelPrice)
    );

}

// =============================== // SUPABASE ALERTS //
===============================

async function getAlerts() { const { data, error } = await supabase
.from(“alerts”) .select(“*“) .eq(”active”, true);

    if (error) {
        console.log("SUPABASE ALERT READ ERROR:", error.message);
        return [];
    }

    return data || [];

}

async function refreshAlerts() { const alerts = await getAlerts();

    activeAlerts = alerts;

    const symbols = new Set();

    for (const alert of alerts) {
        const symbol = normalizeSymbol(alert.symbol);

        // A global alert may omit symbol. In that case it is evaluated
        // against every market symbol supplied by Binance 24h data.
        if (symbol) {
            symbols.add(symbol);
        }
    }

    const nextSymbolsKey = [...symbols].sort().join(",");
    monitoredSymbols = symbols;

    console.log(
        "ALERTS:",
        alerts.length,
        "SYMBOLS:",
        [...symbols].join(", ") || "dynamic"
    );

    if (nextSymbolsKey !== websocketSymbolsKey) {
        websocketSymbolsKey = nextSymbolsKey;
        restartBinanceWebSocket();
    }

}

// =============================== // CONDITION EVALUATION //
===============================

async function fetch24hMarketData() { if (marketRefreshRunning) return;
marketRefreshRunning = true;

    try {
        const response = await fetch(
            "https://fapi.binance.com/fapi/v1/ticker/24hr"
        );

        if (!response.ok) {
            console.log(
                "BINANCE 24H ERROR:",
                response.status,
                response.statusText
            );
            return;
        }

        const rows = await response.json();

        if (!Array.isArray(rows)) return;

        const market = new Map();

        for (const row of rows) {
            const symbol = normalizeSymbol(row.symbol);
            if (!symbol) continue;

            market.set(symbol, {
                price: numeric(row.lastPrice),
                priceChange24h: numeric(row.priceChangePercent),
                turnover24h: numeric(row.quoteVolume),
                volume24h: numeric(row.volume)
            });
        }

        // Keep a lightweight rolling price history for all symbols so
        // price-change conditions can work even when an alert has no fixed symbol.
        for (const [symbol, row] of market.entries()) {
            if (Number.isFinite(row.price)) {
                rememberPrice(symbol, row.price);
            }
        }

        for (const alert of activeAlerts) {
            await evaluateAlert(alert, market);
        }

    } catch (e) {
        console.log("BINANCE 24H FETCH ERROR:", e.message);
    } finally {
        marketRefreshRunning = false;
    }

}

function evaluateCondition(condition, symbol, marketRow, currentPrice) {
if (!condition || typeof condition !== “object”) { return false; }

    const metric = String(condition.metric || "").toLowerCase();
    const operator = String(condition.operator || "").trim();
    const target = numeric(condition.value);

    if (metric === "price_cross") {
        const level = target;
        if (level === null) return false;
        return crossesLevel(symbol, currentPrice, level);
    }

    if (target === null) return false;

    if (metric === "price") {
        return evaluateOperator(currentPrice, operator, target);
    }

    if (metric === "price_change") {
        const timeframe = String(condition.timeframe || "1h").toLowerCase();

        let windowMs = 60 * 60 * 1000;
        if (timeframe === "5m") windowMs = 5 * 60 * 1000;
        if (timeframe === "15m") windowMs = 15 * 60 * 1000;
        if (timeframe === "30m") windowMs = 30 * 60 * 1000;
        if (timeframe === "1h") windowMs = 60 * 60 * 1000;
        if (timeframe === "4h") windowMs = 4 * 60 * 60 * 1000;
        if (timeframe === "24h") windowMs = 24 * 60 * 60 * 1000;

        const previous = previousPrice(symbol, windowMs);
        if (previous === null || previous === 0) return false;

        const changePct = ((currentPrice - previous) / previous) * 100;
        return evaluateOperator(changePct, operator, target);
    }

    if (!marketRow) return false;

    if (metric === "turnover_24h") {
        return evaluateOperator(marketRow.turnover24h, operator, target);
    }

    if (metric === "volume_24h") {
        return evaluateOperator(marketRow.volume24h, operator, target);
    }

    if (metric === "price_change_24h") {
        return evaluateOperator(
            marketRow.priceChange24h,
            operator,
            target
        );
    }

    return false;

}

function evaluateConditionTree(node, symbol, marketRow, currentPrice) {
// A plain array is a convenient shorthand for AND. if
(Array.isArray(node)) { if (!node.length) return false;

        return node.every(item =>
            evaluateConditionTree(
                item,
                symbol,
                marketRow,
                currentPrice
            )
        );
    }

    if (!node || typeof node !== "object") {
        return false;
    }

    // A leaf condition.
    if (node.metric) {
        return evaluateCondition(
            node,
            symbol,
            marketRow,
            currentPrice
        );
    }

    const logic = String(node.logic || "AND").toUpperCase();
    const items = getNestedItems(node);

    if (!items.length) return false;

    if (logic === "OR") {
        return items.some(item =>
            evaluateConditionTree(
                item,
                symbol,
                marketRow,
                currentPrice
            )
        );
    }

    return items.every(item =>
        evaluateConditionTree(
            item,
            symbol,
            marketRow,
            currentPrice
        )
    );

}

function formatConditionForMessage(condition, parts = []) { if
(!condition || typeof condition !== “object”) return parts;

    if (condition.metric) {
        const metric = String(condition.metric);
        const operator = String(condition.operator || "crosses");
        const value = condition.value;

        if (metric === "price_cross") {
            parts.push(`Цена пересекла ${value}`);
        } else if (metric === "price_change") {
            parts.push(
                `Изменение ${condition.timeframe || "1h"} ${operator} ${value}%`
            );
        } else if (metric === "turnover_24h") {
            parts.push(`Оборот 24h ${operator} ${value}`);
        } else if (metric === "volume_24h") {
            parts.push(`Объём 24h ${operator} ${value}`);
        } else if (metric === "price_change_24h") {
            parts.push(`Изменение 24h ${operator} ${value}%`);
        } else {
            parts.push(`${metric} ${operator} ${value}`);
        }

        return parts;
    }

    const logic = String(condition.logic || "AND").toUpperCase();

    for (const item of getNestedItems(condition)) {
        formatConditionForMessage(item, parts);
    }

    return parts;

}

async function evaluateAlert(alert, market) { const alertSymbol =
normalizeSymbol(alert.symbol); const symbols = alertSymbol ?
[alertSymbol] : […market.keys()];

    for (const symbol of symbols) {
        const row = market.get(symbol);
        if (!row || !Number.isFinite(row.price)) continue;

        const hit = evaluateConditionTree(
            alert.conditions,
            symbol,
            row,
            row.price
        );

        if (!hit) continue;

        const cooldown =
            Number(alert.cooldown_seconds || 60) * 1000;

        if (alert.last_trigger_time) {
            const last = new Date(
                alert.last_trigger_time
            ).getTime();

            if (
                Number.isFinite(last) &&
                Date.now() - last < cooldown
            ) {
                continue;
            }
        }

        const title =
            alert.alert_name ||
            alert.name ||
            "Алерт";

        const instrument =
            alert.instrument ||
            "B-F";

        const conditionText =
            formatConditionForMessage(
                alert.conditions,
                []
            ).join(
                String(alert.conditions?.logic || "AND")
                    .toUpperCase() === "OR"
                    ? " ИЛИ "
                    : " И "
            );

        const message =

`🔔 ${title} · ${symbol} · ${instrument}

Цена: ${row.price}

Условие: ${conditionText || “условия выполнены”}

Время: ${localTime()}`;

        const sent = await sendTelegram(message);

        if (!sent) continue;

        const triggeredAt = nowIso();

        const { error } = await supabase
            .from("alerts")
            .update({
                last_trigger_price: row.price,
                last_trigger_time: triggeredAt,
                updated_at: triggeredAt
            })
            .eq("id", alert.id);

        if (error) {
            console.log(
                "SUPABASE ALERT UPDATE ERROR:",
                error.message
            );
        }

        alert.last_trigger_price = row.price;
        alert.last_trigger_time = triggeredAt;
        alert.updated_at = triggeredAt;

        console.log(
            "ALERT TRIGGERED:",
            alert.id,
            symbol,
            row.price
        );
    }

}

// =============================== // BINANCE WEBSOCKET //
===============================

function restartBinanceWebSocket() { if (websocket) { try {
websocket.removeAllListeners(); websocket.close(); } catch (e) {
console.log(“WS CLOSE ERROR:”, e.message); }

        websocket = null;
    }

    if (!monitoredSymbols.size) {
        console.log(
            "BINANCE: no fixed symbols, websocket not started"
        );
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

                if (!msg || !msg.data) return;

                const symbol =
                    normalizeSymbol(msg.data.s);

                const price =
                    numeric(msg.data.p);

                if (!symbol || price === null) return;

                rememberPrice(symbol, price);

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

// =============================== // LOCAL TIME //
===============================

function localTime() { return new Date().toLocaleString( “ru-RU”, {
hour: “2-digit”, minute: “2-digit”, second: “2-digit” } ); }

// =============================== // HTTP SERVER //
===============================

const server = http.createServer( async (req, res) => {

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
            // GET ALERTS
            // ===============================

            if (
                req.method === "GET" &&
                url.pathname === "/api/alerts"
            ) {
                const alerts = await getAlerts();

                res.end(
                    JSON.stringify(alerts)
                );

                return;
            }

            // ===============================
            // POST ALERT
            // ===============================

            if (
                req.method === "POST" &&
                url.pathname === "/api/alerts"
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

                        if (
                            !data.conditions ||
                            typeof data.conditions !== "object"
                        ) {
                            res.statusCode = 400;

                            res.end(
                                JSON.stringify({
                                    ok: false,
                                    error: "conditions are required"
                                })
                            );

                            return;
                        }

                        const alert = {
                            name:
                                String(
                                    data.name ||
                                    data.alert_name ||
                                    "Алерт"
                                ).trim(),

                            alert_name:
                                String(
                                    data.alert_name ||
                                    data.name ||
                                    "Алерт"
                                ).trim(),

                            symbol:
                                normalizeSymbol(data.symbol),

                            exchange:
                                data.exchange ||
                                "Binance",

                            market:
                                data.market ||
                                "Futures",

                            instrument:
                                data.instrument ||
                                "B-F",

                            active: true,

                            conditions:
                                data.conditions,

                            cooldown_seconds:
                                Number(
                                    data.cooldown_seconds || 60
                                ),

                            last_trigger_price: null,
                            last_trigger_time: null
                        };

                        if (
                            !Number.isFinite(
                                alert.cooldown_seconds
                            ) ||
                            alert.cooldown_seconds < 0
                        ) {
                            res.statusCode = 400;

                            res.end(
                                JSON.stringify({
                                    ok: false,
                                    error: "Invalid cooldown_seconds"
                                })
                            );

                            return;
                        }

                        const {
                            data: created,
                            error
                        } = await supabase
                            .from("alerts")
                            .insert(alert)
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
                            "ALERT CREATED:",
                            created.id
                        );

                        await refreshAlerts();

                        res.end(
                            JSON.stringify({
                                ok: true,
                                alert: created
                            })
                        );
                    }
                );

                return;
            }

            res.end(
                JSON.stringify({
                    ok: true,
                    service: "Render Binance Alert Monitor GS"
                })
            );
        }
    );

// =============================== // START SERVER //
===============================

server.listen( process.env.PORT || 10000, () => { console.log( “HTTP
server started” );

        refreshAlerts();

        // Keep alert definitions synchronized with Supabase.
        setInterval(
            refreshAlerts,
            5000
        );

        // 24h ticker supplies turnover/volume/24h change
        // and drives evaluation of all generic conditions.
        setInterval(
            fetch24hMarketData,
            MARKET_REFRESH_MS
        );

        // Run immediately instead of waiting for the first interval.
        fetch24hMarketData();
    }

);
