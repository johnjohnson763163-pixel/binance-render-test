const http = require('http');

const PORT = process.env.PORT || 10000;
const BINANCE_WS_URL =
  'wss://fstream.binance.com/market/stream?streams=solusdt@aggTrade';

let state = 'starting';
let openedAt = null;
let lastMessageAt = null;
let messageCount = 0;
let lastPrice = null;
let lastError = null;
let closeCode = null;
let closeReason = null;

function connectBinance() {
  state = 'connecting';
  lastError = null;
  closeCode = null;
  closeReason = null;

  console.log('Connecting to Binance:', BINANCE_WS_URL);

  let ws;

  try {
    ws = new WebSocket(BINANCE_WS_URL);
  } catch (error) {
    state = 'error';
    lastError = String(error?.message || error);
    console.error('WebSocket constructor error:', error);
    setTimeout(connectBinance, 5000);
    return;
  }

  ws.addEventListener('open', () => {
    state = 'connected';
    openedAt = new Date().toISOString();
    console.log('BINANCE WEBSOCKET OPEN');
  });

  ws.addEventListener('message', (event) => {
    lastMessageAt = new Date().toISOString();
    messageCount += 1;

    try {
      const msg = JSON.parse(event.data);
      const trade = msg?.data;

      if (trade?.p != null) {
        lastPrice = trade.p;
      }

      if (messageCount <= 5 || messageCount % 100 === 0) {
        console.log(
          'BINANCE MESSAGE',
          messageCount,
          'price=',
          lastPrice,
          'event=',
          trade?.e
        );
      }
    } catch (error) {
      console.error('Message parse error:', error);
    }
  });

  ws.addEventListener('error', (event) => {
    state = 'error';
    lastError = String(event?.message || event?.error || event);
    console.error('BINANCE WEBSOCKET ERROR:', lastError);
  });

  ws.addEventListener('close', (event) => {
    state = 'closed';
    closeCode = event.code;
    closeReason = event.reason || '';
    console.log(
      'BINANCE WEBSOCKET CLOSED:',
      closeCode,
      closeReason
    );

    setTimeout(connectBinance, 5000);
  });
}

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    const result = {
      ok: state === 'connected' && messageCount > 0,
      state,
      openedAt,
      lastMessageAt,
      messageCount,
      lastPrice,
      lastError,
      closeCode,
      closeReason,
      binanceWsUrl: BINANCE_WS_URL
    };

    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(result, null, 2));
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('HTTP server listening on port', PORT);
  connectBinance();
});
