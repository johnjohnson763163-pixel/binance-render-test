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

console.log("TELEGRAM CONFIG:", {
    token: !!process.env.TELEGRAM_TOKEN,
    chat: !!process.env.TELEGRAM_CHAT
});

// ===============================
// TELEGRAM
// ===============================

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT = process.env.TELEGRAM_CHAT;
const ALERT_SYNC_SECRET = process.env.ALERT_SYNC_SECRET || "";

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
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    chat_id: TELEGRAM_CHAT,
                    text
                })
            }
        );

        const result = await response.json().catch(() => ({}));

        console.log("TELEGRAM HTTP:", response.status, response.statusText);

        if (!response.ok || result.ok === false) {
            console.log(
                "TELEGRAM SEND ERROR:",
                result.description || response.statusText
            );
            return false;
        }

        console.log("TELEGRAM SENT OK");
        return true;
    } catch (e) {
        console.log("TELEGRAM ERROR:", e.message);
        return false;
    }
}

// ===============================
// STATE
// ===============================

let activeAlerts = [];
let activeLevels = [];
let monitoredSymbols = new Set();

let tradeWebSocket = null;
let tickerWebSocket = null;

let tradeSymbolsKey = "";
let tickerSymbolsKey = "";

const priceHistory = new Map();
const tickerMarket = new Map();

const PRICE_HISTORY_MS = 65 * 60 * 1000;
const REFRESH_MS = 5000;

let refreshRunning = false;

// ===============================
// HELPERS
// ===============================

function normalizeSymbol(value) {
    return String(value || "").trim().toUpperCase();
}

function nowIso() {
    return new Date().toISOString();
}

function numeric(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function authorized(req) {
    if (!ALERT_SYNC_SECRET) return true;
    return req.headers["x-alert-sync-secret"] === ALERT_SYNC_SECRET;
}

function alertSymbols(alert) {
    const list = Array.isArray(alert?.symbols)
        ? alert.symbols.map(normalizeSymbol).filter(Boolean)
        : [];

    if (list.length) return list;

    const one = normalizeSymbol(alert?.symbol);
    return one ? [one] : [];
}

function evaluateOperator(left, operator, right) {
    if (!Number.isFinite(left) || !Number.isFinite(right)) return false;

    switch (operator) {
        case ">": return left > right;
        case ">=": return left >= right;
        case "<": return left < right;
        case "<=": return left <= right;
        case "=":
        case "==": return left === right;
        case "!=": return left !== right;
        default: return false;
    }
}

function rememberPrice(symbol, price, time = Date.now()) {
    const history = priceHistory.get(symbol) || [];

    history.push({
        time,
        price
    });

    const cutoff = time - PRICE_HISTORY_MS;

    while (history.length && history[0].time < cutoff) {
        history.shift();
    }

    priceHistory.set(symbol, history);
}

function previousPrice(symbol, windowMs) {
    const history = priceHistory.get(symbol) || [];
    if (!history.length) return null;

    const cutoff = Date.now() - windowMs;

    for (let i = history.length - 1; i >= 0; i--) {
        if (history[i].time <= cutoff) {
            return history[i].price;
        }
    }

    return null;
}

function crossesLevel(symbol, price, levelPrice) {
    const history = priceHistory.get(symbol) || [];

    if (history.length < 2) return false;

    const previous = history[history.length - 2].price;

    return (
        (previous < levelPrice && price >= levelPrice) ||
        (previous > levelPrice && price <= levelPrice)
    );
}

function getNestedItems(node) {
    return Array.isArray(node?.items) ? node.items : [];
}

// ===============================
// SUPABASE READ
// ===============================

async function getAlerts() {
    const { data, error } = await supabase
        .from("alerts")
        .select("*")
        .eq("active", true);

    if (error) {
        console.log("SUPABASE ALERT READ ERROR:", error.message);
        return [];
    }

    return data || [];
}

async function getActiveLevels() {
    const { data, error } = await supabase
        .from("levels")
        .select("*")
        .eq("active", true)
        .eq("triggered", false);

    if (error) {
        console.log("SUPABASE LEVEL READ ERROR:", error.message);
        return [];
    }

    return data || [];
}

// ===============================
// UNIVERSAL ALERTS
// ===============================

function evaluateCondition(condition, symbol, marketRow, currentPrice) {
    if (!condition || typeof condition !== "object") return false;

    const metric = String(condition.metric || "").toLowerCase();
    const operator = String(condition.operator || "").trim();
    const target = numeric(condition.value);

    // Generic universal signal condition.
    // Crossing is direction-independent.
    if (metric === "price_cross") {
        if (target === null) return false;
        return crossesLevel(symbol, currentPrice, target);
    }

    if (target === null) return false;

    if (metric === "price") {
        return evaluateOperator(currentPrice, operator, target);
    }

    if (metric === "price_change") {
        const timeframe = String(condition.timeframe || "1h").toLowerCase();

        const windows = {
            "5m": 5 * 60 * 1000,
            "15m": 15 * 60 * 1000,
            "30m": 30 * 60 * 1000,
            "1h": 60 * 60 * 1000,
            "4h": 4 * 60 * 60 * 1000,
            "24h": 24 * 60 * 60 * 1000
        };

        const previous = previousPrice(
            symbol,
            windows[timeframe] || windows["1h"]
        );

        if (previous === null || previous === 0) return false;

        const changePct =
            ((currentPrice - previous) / previous) * 100;

        return evaluateOperator(changePct, operator, target);
    }

    if (!marketRow) return false;

    if (metric === "turnover_24h") {
        return evaluateOperator(
            marketRow.turnover24h,
            operator,
            target
        );
    }

    if (metric === "volume_24h") {
        return evaluateOperator(
            marketRow.volume24h,
            operator,
            target
        );
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
    if (Array.isArray(node)) {
        if (!node.length) return false;

        return node.every(item =>
            evaluateConditionTree(
                item,
                symbol,
                marketRow,
                currentPrice
            )
        );
    }

    if (!node || typeof node !== "object") return false;

    if (node.metric) {
        return evaluateCondition(
            node,
            symbol,
            marketRow,
            currentPrice
        );
    }

    const logic =
        String(node.logic || "AND").toUpperCase();

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

function formatConditionForMessage(condition, parts = []) {
    if (!condition || typeof condition !== "object") return parts;

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
            parts.push(
                `Оборот 24h ${operator} ${value}`
            );
        } else if (metric === "volume_24h") {
            parts.push(
                `Объём 24h ${operator} ${value}`
            );
        } else if (metric === "price_change_24h") {
            parts.push(
                `Изменение 24h ${operator} ${value}%`
            );
        } else {
            parts.push(
                `${metric} ${operator} ${value}`
            );
        }

        return parts;
    }

    for (const item of getNestedItems(condition)) {
        formatConditionForMessage(item, parts);
    }

    return parts;
}

// ===============================
// EVENTS
// ===============================

async function recordAlertEvent({
    alertId,
    alertName,
    symbol,
    eventType,
    triggerPrice,
    triggerData,
    notificationText,
    triggeredAt
}) {
    if (!alertId) {
        console.log("SUPABASE ALERT EVENT SKIPPED: missing alert id");
        return false;
    }

    const { error } = await supabase
        .from("alert_events")
        .insert({
            alert_id: alertId,
            alert_name: alertName || "Алерт",
            symbol: symbol || null,
            event_type: eventType || "condition",
            trigger_price: triggerPrice ?? null,
            trigger_data: triggerData || {},
            notification_text: notificationText || "",
            triggered_at: triggeredAt || nowIso()
        });

    if (error) {
        console.log(
            "SUPABASE ALERT EVENT ERROR:",
            error.message
        );
        return false;
    }

    return true;
}

async function getAlertEvents(since, limit = 50) {
    let query = supabase
        .from("alert_events")
        .select("*")
        .order("triggered_at", { ascending: true })
        .limit(limit);

    if (since) {
        query = query.gt("triggered_at", since);
    }

    const { data, error } = await query;

    if (error) throw error;

    return (data || []).map(row => ({
        id: String(row.id),
        alertId: String(row.alert_id || ""),
        alertName: row.alert_name || "Алерт",
        symbol: normalizeSymbol(row.symbol),
        market: "Binance Futures",
        time: row.triggered_at || nowIso(),
        values: row.trigger_data || {},
        conditions: row.trigger_data?.conditions || [],
        filters: row.trigger_data?.conditions || [],
        notificationText: row.notification_text || ""
    }));
}

// ===============================
// SIGNAL LEVEL
// ===============================

async function getOrCreateLevelAlert(level) {
    // The existing Supabase schema has no client_id.
    // We identify the synthetic server alert by a unique name.
    const alertName = "Сигнальный уровень";
    const symbol = normalizeSymbol(level.symbol);

    const { data: existingRows, error: readError } =
        await supabase
            .from("alerts")
            .select("id, symbol, alert_name, conditions")
            .eq("alert_name", alertName)
            .eq("symbol", symbol)
            .eq("active", false)
            .limit(50);

    if (readError) throw readError;

    const levelPrice = Number(level.price);

    const existing = (existingRows || []).find(row => {
        const value =
            numeric(row?.conditions?.items?.[0]?.value);

        return value !== null && value === levelPrice;
    });

    if (existing?.id) {
        return existing.id;
    }

    const row = {
        name: alertName,
        alert_name: alertName,
        exchange: level.exchange || "Binance",
        market: level.market || "Futures",
        instrument: level.instrument || "B-F",
        symbol: symbol || null,
        active: false,
        storage_mode: "server",
        conditions: {
            logic: "AND",
            items: [{
                metric: "price_cross",
                operator: "crosses",
                value: levelPrice
            }]
        },
        cooldown_seconds:
            Number(level.cooldown_seconds || 60)
    };

    const { data: created, error } =
        await supabase
            .from("alerts")
            .insert(row)
            .select("id")
            .single();

    if (error) throw error;

    return created.id;
}

async function checkSignalLevels(symbol, price) {
    const normalizedSymbol = normalizeSymbol(symbol);

    const levels = activeLevels.filter(level =>
        normalizeSymbol(level.symbol) === normalizedSymbol &&
        level.active !== false &&
        level.triggered !== true
    );

    for (const level of levels) {
        const levelPrice = Number(level.price);

        if (!Number.isFinite(levelPrice)) continue;

        if (!crossesLevel(
            normalizedSymbol,
            price,
            levelPrice
        )) {
            continue;
        }

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

        const triggeredAt = nowIso();
        const title = "Сигнальный уровень";
        const instrument = level.instrument || "B-F";

        const message =
`🔔 ${title} · ${normalizedSymbol} · ${instrument}

Цена: ${price}

Уровень: ${levelPrice}

Время: ${localTime()}`;

        // The signal-level event is considered triggered even if
        // Telegram is temporarily unavailable. This keeps Screener
        // notification delivery independent from Telegram delivery.
        const sent = await sendTelegram(message);

        const { error: updateError } =
            await supabase
                .from("levels")
                .update({
                    last_trigger_price: price,
                    last_trigger_time: triggeredAt,
                    updated_at: triggeredAt
                })
                .eq("id", level.id);

        if (updateError) {
            console.log(
                "SUPABASE LEVEL UPDATE ERROR:",
                updateError.message
            );
        }

        level.last_trigger_price = price;
        level.last_trigger_time = triggeredAt;
        level.updated_at = triggeredAt;

        try {
            const alertId =
                await getOrCreateLevelAlert(level);

            await recordAlertEvent({
                alertId,
                alertName: title,
                symbol: normalizedSymbol,
                eventType: "signal_level",
                triggerPrice: price,
                triggerData: {
                    level: levelPrice,
                    condition: "crosses",
                    conditions: [{
                        metric: "price_cross",
                        operator: "crosses",
                        value: levelPrice
                    }],
                    telegramSent: sent
                },
                notificationText: message,
                triggeredAt
            });
        } catch (e) {
            console.log(
                "SIGNAL LEVEL EVENT ERROR:",
                e.message
            );
        }

        console.log(
            "SIGNAL LEVEL TRIGGERED:",
            normalizedSymbol,
            levelPrice,
            price
        );
    }
}

// ===============================
// UNIVERSAL ALERT TRIGGER
// ===============================

async function evaluateAlert(alert, symbol, marketRow, price) {
    const symbols = alertSymbols(alert);

    if (symbols.length && !symbols.includes(symbol)) {
        return;
    }

    const hit = evaluateConditionTree(
        alert.conditions,
        symbol,
        marketRow,
        price
    );

    if (!hit) return;

    const cooldown =
        Number(alert.cooldown_seconds || 60) * 1000;

    if (alert.last_trigger_time) {
        const last =
            new Date(alert.last_trigger_time).getTime();

        if (
            Number.isFinite(last) &&
            Date.now() - last < cooldown
        ) {
            return;
        }
    }

    const title =
        alert.alert_name ||
        alert.name ||
        "Алерт";

    const instrument =
        alert.instrument || "B-F";

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

Цена: ${price}

Условие: ${conditionText || "условия выполнены"}

Время: ${localTime()}`;

    const sent = await sendTelegram(message);
    const triggeredAt = nowIso();

    const { error } =
        await supabase
            .from("alerts")
            .update({
                last_trigger_price: price,
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

    alert.last_trigger_price = price;
    alert.last_trigger_time = triggeredAt;
    alert.updated_at = triggeredAt;

    await recordAlertEvent({
        alertId: alert.id,
        alertName: title,
        symbol,
        eventType: "condition",
        triggerPrice: price,
        triggerData: {
            price,
            conditions: alert.conditions,
            telegramSent: sent
        },
        notificationText: message,
        triggeredAt
    });

    console.log(
        "ALERT TRIGGERED:",
        alert.client_id || alert.id,
        symbol,
        price
    );
}

// ===============================
// BINANCE WEBSOCKETS
// ===============================

function closeWebSocket(ws) {
    if (!ws) return;

    try {
        ws.removeAllListeners();
        ws.close();
    } catch (e) {
        console.log("WS CLOSE ERROR:", e.message);
    }
}

function restartTradeWebSocket() {
    closeWebSocket(tradeWebSocket);
    tradeWebSocket = null;

    if (!monitoredSymbols.size) {
        console.log(
            "BINANCE TRADE: no fixed symbols"
        );
        return;
    }

    const streams = [...monitoredSymbols]
        .map(symbol =>
            `${symbol.toLowerCase()}@aggTrade`
        )
        .join("/");

    // Correct Binance Futures combined stream endpoint.
    const url =
        `wss://fstream.binance.com/stream?streams=${streams}`;

    console.log(
        "BINANCE TRADE CONNECTING:",
        [...monitoredSymbols].join(", ")
    );

    const ws = new WebSocket(url);

    tradeWebSocket = ws;

    ws.on("open", () => {
        console.log("BINANCE TRADE CONNECTED");
    });

    ws.on("message", async data => {
        try {
            const msg = JSON.parse(data.toString());
            const event = msg?.data;

            if (!event) return;

            const symbol = normalizeSymbol(event.s);
            const price = numeric(event.p);

            if (!symbol || price === null) return;

            // The actual realtime price source.
            rememberPrice(
                symbol,
                price,
                Date.now()
            );

            console.log(
                "BINANCE PRICE:",
                symbol,
                price
            );

            await checkSignalLevels(
                symbol,
                price
            );

            const marketRow =
                tickerMarket.get(symbol) || {
                    price,
                    priceChange24h: null,
                    turnover24h: null,
                    volume24h: null
                };

            for (const alert of activeAlerts) {
                await evaluateAlert(
                    alert,
                    symbol,
                    marketRow,
                    price
                );
            }
        } catch (e) {
            console.log(
                "BINANCE TRADE MESSAGE ERROR:",
                e.message
            );
        }
    });

    ws.on("error", e => {
        console.log(
            "BINANCE TRADE WS ERROR:",
            e.message
        );
    });

    ws.on("close", () => {
        console.log(
            "BINANCE TRADE DISCONNECTED"
        );

        if (tradeWebSocket === ws) {
            tradeWebSocket = null;
        }

        setTimeout(() => {
            if (
                monitoredSymbols.size &&
                tradeWebSocket === null
            ) {
                restartTradeWebSocket();
            }
        }, 3000);
    });
}

function restartTickerWebSocket() {
    closeWebSocket(tickerWebSocket);
    tickerWebSocket = null;

    if (!monitoredSymbols.size) return;

    const streams = [...monitoredSymbols]
        .map(symbol =>
            `${symbol.toLowerCase()}@ticker`
        )
        .join("/");

    const url =
        `wss://fstream.binance.com/stream?streams=${streams}`;

    console.log(
        "BINANCE TICKER CONNECTING:",
        [...monitoredSymbols].join(", ")
    );

    const ws = new WebSocket(url);

    tickerWebSocket = ws;

    ws.on("open", () => {
        console.log(
            "BINANCE TICKER CONNECTED"
        );
    });

    ws.on("message", async data => {
        try {
            const raw = data.toString();
            const msg = JSON.parse(raw);
            const event = msg?.data;

            if (!event) {
                console.log("BINANCE TICKER MESSAGE WITHOUT DATA");
                return;
            }

            const symbol = normalizeSymbol(event.s);
            const price = numeric(event.c);
            if (!symbol || price === null) return;

            const row = {
                price,
                priceChange24h: numeric(event.P),
                turnover24h: numeric(event.q),
                volume24h: numeric(event.v)
            };

            tickerMarket.set(symbol, row);

            // IMPORTANT: use the Futures ticker stream as a second
            // realtime price source for signal-level crossing.
            // This makes the monitor independent from aggTrade delivery.
            rememberPrice(symbol, price, Date.now());

            console.log("BINANCE TICKER PRICE:", symbol, price);

            await checkSignalLevels(symbol, price);

            for (const alert of activeAlerts) {
                await evaluateAlert(
                    alert,
                    symbol,
                    row,
                    price
                );
            }
        } catch (e) {
            console.log(
                "BINANCE TICKER MESSAGE ERROR:",
                e.message
            );
        }
    });

    ws.on("error", e => {
        console.log(
            "BINANCE TICKER WS ERROR:",
            e.message
        );
    });

    ws.on("close", () => {
        console.log(
            "BINANCE TICKER DISCONNECTED"
        );

        if (tickerWebSocket === ws) {
            tickerWebSocket = null;
        }

        setTimeout(() => {
            if (
                monitoredSymbols.size &&
                tickerWebSocket === null
            ) {
                restartTickerWebSocket();
            }
        }, 3000);
    });
}

function restartBinanceWebSockets() {
    restartTradeWebSocket();
    restartTickerWebSocket();
}

// ===============================
// REFRESH ALERTS / LEVELS
// ===============================

async function refreshAlerts() {
    if (refreshRunning) return;
    refreshRunning = true;

    try {
        const alerts = await getAlerts();
        const levels = await getActiveLevels();

        activeAlerts = alerts;
        activeLevels = levels;

        const symbols = new Set();

        for (const alert of alerts) {
            for (const symbol of alertSymbols(alert)) {
                symbols.add(symbol);
            }
        }

        for (const level of levels) {
            const symbol =
                normalizeSymbol(level.symbol);

            if (symbol) symbols.add(symbol);
        }

        monitoredSymbols = symbols;

        const nextKey =
            [...symbols].sort().join(",");

        console.log(
            "ALERTS:",
            alerts.length,
            "LEVELS:",
            levels.length,
            "SYMBOLS:",
            [...symbols].join(", ") || "dynamic"
        );

        if (nextKey !== tradeSymbolsKey) {
            tradeSymbolsKey = nextKey;
            restartTradeWebSocket();
        }

        if (nextKey !== tickerSymbolsKey) {
            tickerSymbolsKey = nextKey;
            restartTickerWebSocket();
        }
    } catch (e) {
        console.log(
            "REFRESH ERROR:",
            e.message
        );
    } finally {
        refreshRunning = false;
    }
}

// ===============================
// HTTP
// ===============================

const server = http.createServer(
    async (req, res) => {
        res.setHeader(
            "Content-Type",
            "application/json; charset=utf-8"
        );

        const url = new URL(
            req.url,
            "http://localhost"
        );

        // GET EVENTS
        if (
            req.method === "GET" &&
            url.pathname === "/api/alerts/events"
        ) {
            if (!authorized(req)) {
                res.statusCode = 401;
                res.end(JSON.stringify({
                    ok: false,
                    error: "Unauthorized"
                }));
                return;
            }

            try {
                const limit = Math.min(
                    Math.max(
                        Number(
                            url.searchParams.get("limit") || 50
                        ),
                        1
                    ),
                    200
                );

                const events =
                    await getAlertEvents(
                        url.searchParams.get("since") || "",
                        limit
                    );

                res.end(JSON.stringify({
                    ok: true,
                    events
                }));
            } catch (error) {
                res.statusCode = 500;
                res.end(JSON.stringify({
                    ok: false,
                    error: error.message,
                    events: []
                }));
            }

            return;
        }

        // GET ALERTS
        if (
            req.method === "GET" &&
            url.pathname === "/api/alerts"
        ) {
            const alerts = await getAlerts();
            res.end(JSON.stringify(alerts));
            return;
        }

        // POST SIGNAL LEVEL SYNC
        // The Screener sends the complete current set of chart signal levels.
        // Render writes that exact set to Supabase and therefore monitors only
        // the symbols that currently have a signal level in the Screener.
        if (
            req.method === "POST" &&
            url.pathname === "/api/signal-levels/sync"
        ) {

            let body = "";

            req.on("data", chunk => {
                body += chunk;
                if (body.length > 2_000_000) req.destroy();
            });

            req.on("end", async () => {
                try {
                    const data = JSON.parse(body || "{}");
                    if (!Array.isArray(data.levels)) {
                        throw new Error("levels must be an array");
                    }

                    const incoming = [];
                    const seen = new Set();

                    for (const item of data.levels) {
                        const symbol = normalizeSymbol(item?.symbol);
                        const price = numeric(item?.price);
                        if (!symbol || price === null || price <= 0) continue;

                        const key = `${symbol}:${price}`;
                        if (seen.has(key)) continue;
                        seen.add(key);
                        incoming.push({
                            symbol,
                            price,
                            active: item?.active !== false
                        });
                    }

                    // First deactivate everything currently stored. This is
                    // deliberate: the request represents the complete current
                    // Screener state, so removed chart levels must stop being
                    // monitored immediately.
                    const { error: deactivateError } = await supabase
                        .from("levels")
                        .update({
                            active: false,
                            triggered: false,
                            updated_at: nowIso()
                        })
                        .eq("active", true);

                    if (deactivateError) {
                        throw new Error(`Supabase level deactivate: ${deactivateError.message}`);
                    }

                    // Reactivate/update matching levels, or create them if they
                    // do not exist yet. Matching is by symbol + price because
                    // the browser drawing id is not the numeric Supabase id.
                    const { data: existingRows, error: readError } = await supabase
                        .from("levels")
                        .select("*");

                    if (readError) {
                        throw new Error(`Supabase level read: ${readError.message}`);
                    }

                    const existingByKey = new Map();
                    for (const row of existingRows || []) {
                        const key = `${normalizeSymbol(row.symbol)}:${numeric(row.price)}`;
                        if (!existingByKey.has(key)) existingByKey.set(key, row);
                    }

                    let synced = 0;
                    for (const level of incoming) {
                        const key = `${level.symbol}:${level.price}`;
                        const existing = existingByKey.get(key);

                        if (existing?.id) {
                            const { error } = await supabase
                                .from("levels")
                                .update({
                                    symbol: level.symbol,
                                    price: level.price,
                                    type: "signal",
                                    active: level.active,
                                    triggered: false,
                                    alert_name: "Сигнальный уровень",
                                    alert_type: "level",
                                    condition: "cross",
                                    updated_at: nowIso()
                                })
                                .eq("id", existing.id);

                            if (error) throw new Error(`Supabase level update: ${error.message}`);
                        } else {
                            const { error } = await supabase
                                .from("levels")
                                .insert({
                                    symbol: level.symbol,
                                    price: level.price,
                                    type: "signal",
                                    active: level.active,
                                    exchange: "Binance",
                                    market: "Futures",
                                    instrument: "B-F",
                                    alert_name: "Сигнальный уровень",
                                    alert_type: "level",
                                    condition: "cross",
                                    triggered: false,
                                    cooldown_seconds: 60,
                                    updated_at: nowIso()
                                });

                            if (error) throw new Error(`Supabase level insert: ${error.message}`);
                        }

                        if (level.active) synced += 1;
                    }

                    console.log(
                        "SIGNAL LEVEL SYNC:",
                        incoming.map(x => `${x.symbol}@${x.price}`).join(", ") || "none"
                    );

                    // Force the monitor to rebuild its Binance subscriptions from
                    // the freshly synchronized Supabase state.
                    await refreshAlerts();

                    res.end(JSON.stringify({
                        ok: true,
                        levels: synced,
                        symbols: [...new Set(incoming.filter(x => x.active).map(x => x.symbol))]
                    }));
                } catch (error) {
                    console.log("SIGNAL LEVEL SYNC ERROR:", error.message);
                    res.statusCode = 500;
                    res.end(JSON.stringify({
                        ok: false,
                        error: error.message
                    }));
                }
            });

            return;
        }

        // POST FULL ALERT SYNC
        if (
            req.method === "POST" &&
            url.pathname === "/api/alerts/sync"
        ) {
            if (!authorized(req)) {
                res.statusCode = 401;
                res.end(JSON.stringify({
                    ok: false,
                    error: "Unauthorized"
                }));
                return;
            }

            let body = "";

            req.on("data", chunk => {
                body += chunk;

                if (body.length > 2_000_000) {
                    req.destroy();
                }
            });

            req.on("end", async () => {
                try {
                    const data =
                        JSON.parse(body || "{}");

                    if (!Array.isArray(data.alerts)) {
                        throw new Error(
                            "alerts must be an array"
                        );
                    }

                    // Current production schema intentionally uses
                    // only columns that actually exist in alerts.
                    // We sync by alert id when supplied, otherwise insert.
                    let synced = 0;

                    for (const incoming of data.alerts) {
                        const conditions =
                            incoming.conditions;

                        if (
                            !conditions ||
                            typeof conditions !== "object"
                        ) {
                            continue;
                        }

                        const row = {
                            name: String(
                                incoming.name ||
                                incoming.alert_name ||
                                "Алерт"
                            ).trim(),

                            alert_name: String(
                                incoming.alert_name ||
                                incoming.name ||
                                "Алерт"
                            ).trim(),

                            exchange:
                                incoming.exchange ||
                                "Binance",

                            market:
                                incoming.market ||
                                "Futures",

                            instrument:
                                incoming.instrument ||
                                "B-F",

                            symbol:
                                normalizeSymbol(
                                    incoming.symbol
                                ) || null,

                            active:
                                incoming.active !== false,

                            storage_mode:
                                incoming.storage_mode ||
                                "local_server",

                            conditions,

                            cooldown_seconds:
                                Math.max(
                                    0,
                                    Number(
                                        incoming.cooldown_seconds ??
                                        60
                                    )
                                ),

                            updated_at: nowIso()
                        };

                        if (
                            !Number.isFinite(
                                row.cooldown_seconds
                            )
                        ) {
                            continue;
                        }

                        const numericId =
                            Number(incoming.id);

                        if (
                            Number.isInteger(numericId) &&
                            numericId > 0
                        ) {
                            const {
                                data: existing
                            } = await supabase
                                .from("alerts")
                                .select("id")
                                .eq("id", numericId)
                                .maybeSingle();

                            if (existing?.id) {
                                const { error } =
                                    await supabase
                                        .from("alerts")
                                        .update(row)
                                        .eq(
                                            "id",
                                            existing.id
                                        );

                                if (error) throw error;

                                synced++;
                                continue;
                            }
                        }

                        const { error } =
                            await supabase
                                .from("alerts")
                                .insert(row);

                        if (error) throw error;

                        synced++;
                    }

                    await refreshAlerts();

                    res.end(JSON.stringify({
                        ok: true,
                        alerts: synced
                    }));
                } catch (error) {
                    res.statusCode = 500;
                    res.end(JSON.stringify({
                        ok: false,
                        error: error.message
                    }));
                }
            });

            return;
        }

        // POST SINGLE ALERT
        if (
            req.method === "POST" &&
            url.pathname === "/api/alerts"
        ) {
            if (!authorized(req)) {
                res.statusCode = 401;
                res.end(JSON.stringify({
                    ok: false,
                    error: "Unauthorized"
                }));
                return;
            }

            let body = "";

            req.on("data", chunk => {
                body += chunk;
            });

            req.on("end", async () => {
                try {
                    const data =
                        JSON.parse(body || "{}");

                    if (
                        !data ||
                        typeof data !== "object" ||
                        Array.isArray(data)
                    ) {
                        throw new Error(
                            "Invalid JSON object"
                        );
                    }

                    if (
                        !data.conditions ||
                        typeof data.conditions !== "object"
                    ) {
                        throw new Error(
                            "conditions are required"
                        );
                    }

                    const cooldown =
                        Number(
                            data.cooldown_seconds ?? 60
                        );

                    if (
                        !Number.isFinite(cooldown) ||
                        cooldown < 0
                    ) {
                        throw new Error(
                            "Invalid cooldown_seconds"
                        );
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
                            normalizeSymbol(
                                data.symbol
                            ) || null,

                        active: true,

                        storage_mode:
                            data.storage_mode ||
                            "local_server",

                        exchange:
                            data.exchange ||
                            "Binance",

                        market:
                            data.market ||
                            "Futures",

                        instrument:
                            data.instrument ||
                            "B-F",

                        conditions:
                            data.conditions,

                        cooldown_seconds:
                            cooldown,

                        last_trigger_price: null,
                        last_trigger_time: null
                    };

                    const {
                        data: created,
                        error
                    } = await supabase
                        .from("alerts")
                        .insert(alert)
                        .select("*")
                        .single();

                    if (error) throw error;

                    console.log(
                        "ALERT CREATED:",
                        created.id
                    );

                    await refreshAlerts();

                    res.end(JSON.stringify({
                        ok: true,
                        alert: created
                    }));
                } catch (error) {
                    res.statusCode = 500;
                    res.end(JSON.stringify({
                        ok: false,
                        error: error.message
                    }));
                }
            });

            return;
        }

        res.end(JSON.stringify({
            ok: true,
            service: "Render Binance Alert Monitor GS"
        }));
    }
);

// ===============================
// START
// ===============================

server.listen(
    process.env.PORT || 10000,
    () => {
        console.log("HTTP server started");

        refreshAlerts();

        setInterval(
            refreshAlerts,
            REFRESH_MS
        );
    }
);
