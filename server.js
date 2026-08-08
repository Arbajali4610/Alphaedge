const express = require('express');
const UpstoxClient = require('upstox-js-sdk');

const app = express();
const PORT = process.env.PORT || 10000;
const ACCESS_TOKEN = process.env.UPSTOX_ACCESS_TOKEN;

const INSTRUMENTS = {
  nifty: 'NSE_INDEX|Nifty 50',
  sensex: 'BSE_INDEX|SENSEX',
  banknifty: 'NSE_INDEX|Nifty Bank'
};

let latest = {
  nifty: null,
  sensex: null,
  banknifty: null,
  connected: false,
  updatedAt: null,
  error: ACCESS_TOKEN ? null : 'UPSTOX_ACCESS_TOKEN is not configured'
};

const clients = new Set();

app.use(express.static(__dirname));

app.get('/api/market', (req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json(latest);
});

app.get('/api/market-stream', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });
  res.flushHeaders();
  res.write(`data: ${JSON.stringify(latest)}\n\n`);
  clients.add(res);
  req.on('close', () => clients.delete(res));
});

function broadcast() {
  const payload = `data: ${JSON.stringify(latest)}\n\n`;
  for (const res of clients) {
    try { res.write(payload); } catch (_) { clients.delete(res); }
  }
}

function extractLtpc(feed) {
  if (!feed || typeof feed !== 'object') return null;
  if (feed.ltpc) return feed.ltpc;
  if (feed.ff?.indexFF?.ltpc) return feed.ff.indexFF.ltpc;
  if (feed.fullFeed?.indexFF?.ltpc) return feed.fullFeed.indexFF.ltpc;
  if (feed.fullFeed?.marketFF?.ltpc) return feed.fullFeed.marketFF.ltpc;
  if (feed.indexFF?.ltpc) return feed.indexFF.ltpc;
  return null;
}

function applyFeed(message) {
  let data = message;
  try {
    if (Buffer.isBuffer(data)) data = data.toString('utf8');
    if (typeof data === 'string') data = JSON.parse(data);
  } catch (_) {
    return;
  }

  const feeds = data?.feeds || data?.data?.feeds;
  if (!feeds || typeof feeds !== 'object') return;

  for (const [key, feed] of Object.entries(feeds)) {
    let name = null;
    if (key === INSTRUMENTS.nifty) name = 'nifty';
    else if (key === INSTRUMENTS.sensex) name = 'sensex';
    else if (key === INSTRUMENTS.banknifty) name = 'banknifty';
    if (!name) continue;

    const ltpc = extractLtpc(feed);
    if (!ltpc || typeof ltpc.ltp !== 'number') continue;

    const previousClose = Number(ltpc.cp);
    const changePct = Number.isFinite(previousClose) && previousClose !== 0
      ? ((ltpc.ltp - previousClose) / previousClose) * 100
      : null;

    latest[name] = {
      ltp: ltpc.ltp,
      close: Number.isFinite(previousClose) ? previousClose : null,
      changePct,
      ltt: ltpc.ltt || null
    };
    latest.updatedAt = Date.now();
    latest.error = null;
  }

  broadcast();
}

function startUpstox() {
  if (!ACCESS_TOKEN) {
    console.warn('Live market feed disabled: set UPSTOX_ACCESS_TOKEN in Render Environment Variables.');
    return;
  }

  try {
    const defaultClient = UpstoxClient.ApiClient.instance;
    const oauth = defaultClient.authentications['OAUTH2'];
    oauth.accessToken = ACCESS_TOKEN;

    const streamer = new UpstoxClient.MarketDataStreamerV3(
      [INSTRUMENTS.nifty, INSTRUMENTS.sensex, INSTRUMENTS.banknifty],
      'ltpc'
    );

    streamer.on('open', () => {
      latest.connected = true;
      latest.error = null;
      broadcast();
      try {
        streamer.subscribe(
          [INSTRUMENTS.nifty, INSTRUMENTS.sensex, INSTRUMENTS.banknifty],
          'ltpc'
        );
      } catch (err) {
        latest.error = `Subscription error: ${err.message}`;
        broadcast();
      }
    });

    streamer.on('message', applyFeed);

    streamer.on('error', (err) => {
      latest.connected = false;
      latest.error = err?.message || 'Market feed connection error';
      broadcast();
    });

    streamer.on('close', () => {
      latest.connected = false;
      latest.error = 'Market feed disconnected';
      broadcast();
    });

    streamer.connect();
  } catch (err) {
    latest.connected = false;
    latest.error = err.message || 'Unable to start market feed';
    broadcast();
    console.error(err);
  }
}

app.listen(PORT, () => {
  console.log(`AlphaEdge running on port ${PORT}`);
  startUpstox();
});
