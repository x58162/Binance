require("dotenv").config();

const express = require("express");
const crypto = require("crypto");
const WebSocket = require("ws");
const path = require("path");

const app = express();
const PORT = Number(process.env.PORT || 6092);
const API_KEY = (process.env.BINANCE_API_KEY || "").trim();
const API_SECRET = (process.env.BINANCE_API_SECRET || "").trim();
const REST_BASE = (process.env.BINANCE_REST_BASE || "https://fapi.binance.com").trim().replace(/\/$/, "");
const WS_BASE = (process.env.BINANCE_WS_BASE || "wss://fstream.binance.com/private").trim().replace(/\/$/, "");
const BTCUSDC_ONLY = String(process.env.BTCUSDC_ONLY || "false").trim().toLowerCase() === "true";

app.use(express.static(path.join(__dirname, "public")));
app.locals.clients = new Set();

let listenKey = null;
let userSocket = null;
let marketSocket = null;
let marketReconnectTimer = null;
let marketGeneration = 0;
let activeMarketSymbols = "";
let marketState = "尚未連線";
let keepAliveTimer = null;
let reconnectTimer = null;
let snapshotRetryTimer = null;
let reconnectDelay = 3000;
let isStopping = false;
let lastEventAt = null;
let restState = "尚未讀取";
let streamState = "尚未連線";
let lastError = null;
const positions = new Map();

function overallState() {
  if (restState === "讀取失敗") return "REST 讀取失敗｜串流" + streamState;
  if (restState === "讀取中") return "REST 讀取中｜串流" + streamState;
  if (restState === "已讀取") return "REST 已讀取｜串流" + streamState;
  return "REST " + restState + "｜帳戶串流" + streamState + "｜行情" + marketState;
}

function positionKey(symbol, side) {
  return symbol + ":" + (side || "BOTH");
}

function isAllowedSymbol(symbol) {
  return !BTCUSDC_ONLY || symbol === "BTCUSDC";
}

function sendToBrowsers(type, payload) {
  const message = JSON.stringify({ type, payload, serverTime: Date.now() });
  for (const client of app.locals.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(message);
  }
}

function publishStatus() {
  sendToBrowsers("status", {
    state: overallState(),
    restState,
    streamState,
    marketState,
    error: lastError
  });
}

function publicPositions() {
  return Array.from(positions.values())
    .filter(p => isAllowedSymbol(p.symbol))
    .sort((a, b) => a.symbol.localeCompare(b.symbol) || a.positionSide.localeCompare(b.positionSide));
}

function publishPositions() {
  sendToBrowsers("positions", publicPositions());
}

function syncMarketStream() {
  const symbols = [...new Set(publicPositions().map(p => p.symbol.toLowerCase()))].sort();
  const next = symbols.join(",");
  if (next === activeMarketSymbols && marketSocket && marketSocket.readyState <= WebSocket.OPEN) return;
  if (next === activeMarketSymbols && marketReconnectTimer) return;
  activeMarketSymbols = next;
  marketGeneration += 1;
  const generation = marketGeneration;
  if (marketReconnectTimer) {
    clearTimeout(marketReconnectTimer);
    marketReconnectTimer = null;
  }
  if (marketSocket) {
    const old = marketSocket;
    marketSocket = null;
    old.removeAllListeners("close");
    old.close();
  }
  if (!symbols.length) {
    marketState = "等待持倉";
    publishStatus();
    return;
  }
  connectMarketStream(symbols, generation);
}

function connectMarketStream(symbols, generation) {
  const streams = symbols.map(symbol => symbol + "@markPrice@1s").join("/");
  const url = "wss://fstream.binance.com/stream?streams=" + streams;
  marketState = "連線中";
  publishStatus();
  const socket = new WebSocket(url);
  marketSocket = socket;
  socket.on("open", () => {
    if (generation !== marketGeneration) return;
    marketState = "已連線";
    console.log("Binance Mark Price Stream 已連線：" + symbols.join(", "));
    publishStatus();
  });
  socket.on("message", raw => {
    if (generation !== marketGeneration) return;
    let message;
    try { message = JSON.parse(raw.toString()); } catch { return; }
    const event = message.data || message;
    if (event.e !== "markPriceUpdate" || !event.s || event.p === undefined) return;
    const mark = Number(event.p);
    if (!Number.isFinite(mark)) return;
    let changed = false;
    for (const [key, position] of positions) {
      if (position.symbol !== event.s) continue;
      const amount = Number(position.positionAmt || 0);
      const entry = Number(position.entryPrice || 0);
      position.markPrice = String(mark);
      position.unrealizedProfit = String((mark - entry) * amount);
      position.markPriceEventTime = event.E || Date.now();
      positions.set(key, position);
      changed = true;
    }
    if (changed) {
      lastEventAt = event.E || Date.now();
      publishPositions();
    }
  });
  socket.on("error", err => {
    if (generation !== marketGeneration) return;
    marketState = "連線錯誤";
    lastError = "Mark Price Stream：" + err.message;
    publishStatus();
  });
  socket.on("close", () => {
    if (generation !== marketGeneration || isStopping) return;
    marketSocket = null;
    marketState = "已斷線，重新連線中";
    publishStatus();
    if (marketReconnectTimer) clearTimeout(marketReconnectTimer);
    marketReconnectTimer = setTimeout(() => {
      marketReconnectTimer = null;
      if (generation === marketGeneration) connectMarketStream(symbols, generation);
    }, 3000);
  });
}

function createSignedQuery(params = {}) {
  if (!API_SECRET) throw new Error("尚未設定 BINANCE_API_SECRET");
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) query.append(key, String(value));
  }
  query.append("recvWindow", "10000");
  query.append("timestamp", String(Date.now()));
  const queryString = query.toString();
  const signature = crypto.createHmac("sha256", API_SECRET).update(queryString, "utf8").digest("hex");
  return { queryString, signature };
}

async function signedRequest(method, endpoint, params = {}) {
  if (!API_KEY || !API_SECRET) throw new Error("尚未設定 Binance API Key / Secret");
  const signed = createSignedQuery(params);
  const url = REST_BASE + endpoint + "?" + signed.queryString + "&signature=" + signed.signature;
  const response = await fetch(url, {
    method,
    headers: { "X-MBX-APIKEY": API_KEY }
  });

  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error("Binance 回傳非 JSON：HTTP " + response.status);
  }

  if (!response.ok) {
    if (Number(data.code) === -1022) {
      throw new Error("Binance API -1022：簽名不一致。請確認 API Key/Secret 是同一組、Secret 完整且沒有多餘字元；不要貼出 Secret。");
    }
    if (Number(data.code) === -1021) {
      throw new Error("Binance API -1021：時間戳超出允許範圍，請確認主機時間同步。");
    }
    throw new Error("Binance API " + (data.code ?? response.status) + ": " + (data.msg || response.statusText));
  }
  return data;
}

async function apiKeyRequest(method, endpoint) {
  if (!API_KEY) throw new Error("尚未設定 BINANCE_API_KEY");
  const response = await fetch(REST_BASE + endpoint, {
    method,
    headers: { "X-MBX-APIKEY": API_KEY }
  });
  const text = await response.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch {}
  if (!response.ok) throw new Error("listenKey API " + (data.code ?? response.status) + ": " + (data.msg || response.statusText));
  return data;
}

function mapRestPosition(p) {
  return {
    symbol: p.symbol,
    positionSide: p.positionSide || "BOTH",
    positionAmt: String(p.positionAmt ?? "0"),
    entryPrice: String(p.entryPrice ?? "0"),
    breakEvenPrice: String(p.breakEvenPrice ?? "0"),
    markPrice: String(p.markPrice ?? "0"),
    unrealizedProfit: String(p.unRealizedProfit ?? "0"),
    liquidationPrice: String(p.liquidationPrice ?? "0"),
    leverage: String(p.leverage ?? ""),
    marginType: String(p.marginType ?? ""),
    isolatedMargin: String(p.isolatedMargin ?? "0"),
    isolatedWallet: String(p.isolatedWallet ?? "0"),
    notional: String(p.notional ?? "0"),
    source: "REST 快照"
  };
}

function applyAccountUpdate(event) {
  for (const p of event?.a?.P || []) {
    const symbol = p.s;
    const side = p.ps || "BOTH";
    if (!symbol) continue;
    const key = positionKey(symbol, side);
    const amount = Number(p.pa || 0);
    if (amount === 0) {
      positions.delete(key);
      continue;
    }
    const old = positions.get(key) || {};
    positions.set(key, {
      ...old,
      symbol,
      positionSide: side,
      positionAmt: String(p.pa ?? old.positionAmt ?? "0"),
      entryPrice: String(p.ep ?? old.entryPrice ?? "0"),
      breakEvenPrice: String(p.bep ?? old.breakEvenPrice ?? "0"),
      unrealizedProfit: String(p.up ?? old.unrealizedProfit ?? "0"),
      marginType: String(p.mt ?? old.marginType ?? ""),
      isolatedWallet: String(p.iw ?? old.isolatedWallet ?? "0"),
      source: "WebSocket 更新"
    });
  }
  lastEventAt = Date.now();
  publishPositions();
  syncMarketStream();
}

async function loadInitialSnapshot() {
  restState = "讀取中";
  publishStatus();
  const data = await signedRequest("GET", "/fapi/v3/positionRisk");
  if (!Array.isArray(data)) throw new Error("Binance 持倉 API 回傳格式不符合預期");

  positions.clear();
  for (const raw of data) {
    const p = mapRestPosition(raw);
    if (Number(p.positionAmt) !== 0) positions.set(positionKey(p.symbol, p.positionSide), p);
  }

  restState = "已讀取";
  lastError = null;
  publishPositions();
  syncMarketStream();
  publishStatus();
  console.log("已取得初始持倉快照：" + positions.size + " 筆非零持倉");
}

function scheduleSnapshotRetry() {
  if (isStopping || snapshotRetryTimer) return;
  snapshotRetryTimer = setTimeout(async () => {
    snapshotRetryTimer = null;
    try {
      await loadInitialSnapshot();
    } catch (err) {
      restState = "讀取失敗";
      lastError = err.message;
      console.error("REST 持倉快照讀取失敗：", err.message);
      publishStatus();
      scheduleSnapshotRetry();
    }
  }, 30000);
}

async function createListenKey() {
  const data = await apiKeyRequest("POST", "/fapi/v1/listenKey");
  if (!data.listenKey) throw new Error("Binance 沒有回傳 listenKey");
  listenKey = data.listenKey;
  console.log("listenKey 已建立（不會輸出 key 值）");
}

async function keepAlive() {
  try {
    await apiKeyRequest("PUT", "/fapi/v1/listenKey");
    console.log("listenKey 延長成功");
  } catch (err) {
    console.error("listenKey 延長失敗：", err.message);
    if (String(err.message).includes("-1125")) await restartUserStream();
  }
}

function scheduleReconnect() {
  if (isStopping || reconnectTimer) return;
  streamState = "重新連線中";
  publishStatus();
  reconnectTimer = setTimeout(async () => {
    reconnectTimer = null;
    try {
      await connectUserStream();
    } catch (err) {
      lastError = err.message;
      streamState = "連線失敗";
      console.error("重新連線失敗：", err.message);
      publishStatus();
      scheduleReconnect();
    }
  }, reconnectDelay);
  reconnectDelay = Math.min(Math.round(reconnectDelay * 1.7), 30000);
}

function connectSocket(key) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(WS_BASE + "/ws/" + key);
    userSocket = socket;
    let opened = false;
    const timeout = setTimeout(() => {
      if (!opened) {
        socket.terminate();
        reject(new Error("Binance User Data Stream WebSocket 連線逾時"));
      }
    }, 15000);

    socket.on("open", () => {
      opened = true;
      clearTimeout(timeout);
      reconnectDelay = 3000;
      streamState = "已連線";
      console.log("Binance User Data Stream 已連線");
      publishStatus();
      resolve();
    });

    socket.on("message", raw => {
      let event;
      try { event = JSON.parse(raw.toString()); } catch { return; }
      if (event.e === "ACCOUNT_UPDATE") {
        applyAccountUpdate(event);
        console.log("收到 ACCOUNT_UPDATE", new Date(event.E).toLocaleTimeString());
      } else if (event.e === "ORDER_TRADE_UPDATE") {
        lastEventAt = Date.now();
        sendToBrowsers("order", {
          symbol: event.o?.s,
          side: event.o?.S,
          status: event.o?.X,
          executionType: event.o?.x,
          quantity: event.o?.q,
          averagePrice: event.o?.ap,
          eventTime: event.E
        });
      } else if (event.e === "listenKeyExpired") {
        console.warn("listenKey 已到期，準備重新建立");
        restartUserStream().catch(err => {
          lastError = err.message;
          scheduleReconnect();
        });
      }
    });

    socket.on("error", err => {
      lastError = err.message;
      if (!opened) {
        clearTimeout(timeout);
        reject(err);
      }
    });

    socket.on("close", () => {
      if (userSocket === socket) userSocket = null;
      if (!isStopping) {
        streamState = "已斷線";
        publishStatus();
        scheduleReconnect();
      }
    });
  });
}

async function connectUserStream() {
  streamState = "取得 listenKey";
  publishStatus();
  await createListenKey();
  await connectSocket(listenKey);
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  keepAliveTimer = setInterval(() => keepAlive().catch(err => {
    lastError = err.message;
    console.error(err.message);
    publishStatus();
  }), 30 * 60 * 1000);
}

async function restartUserStream() {
  if (userSocket) {
    userSocket.removeAllListeners("close");
    userSocket.close();
    userSocket = null;
  }
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  listenKey = null;
  await connectUserStream();
}

const httpServer = app.listen(PORT, async () => {
  console.log("本機持倉監控頁面：http://localhost:" + PORT);
  if (!API_KEY || !API_SECRET) {
    restState = "尚未設定 .env";
    streamState = "未啟動";
    console.log("請複製 .env.example 為 .env，並填入 API Key / Secret。");
    publishStatus();
    return;
  }

  try {
    await loadInitialSnapshot();
  } catch (err) {
    restState = "讀取失敗";
    lastError = err.message;
    console.error("啟動時 REST 持倉快照失敗：", err.message);
    publishStatus();
    scheduleSnapshotRetry();
  }

  try {
    await connectUserStream();
  } catch (err) {
    lastError = err.message;
    streamState = "連線失敗";
    console.error("User Data Stream 啟動失敗：", err.message);
    publishStatus();
    scheduleReconnect();
  }
});

const browserWss = new WebSocket.Server({ server: httpServer, path: "/browser-ws" });
browserWss.on("connection", client => {
  app.locals.clients.add(client);
  client.send(JSON.stringify({
    type: "status",
    payload: { state: overallState(), restState, streamState, marketState, error: lastError },
    serverTime: Date.now()
  }));
  client.send(JSON.stringify({ type: "positions", payload: publicPositions(), serverTime: Date.now() }));
  client.on("close", () => app.locals.clients.delete(client));
});

app.get("/api/status", (_req, res) => res.json({
  state: overallState(),
  restState,
  streamState,
  marketState,
  lastEventAt,
  positionCount: publicPositions().length,
  error: lastError
}));
app.get("/api/positions", (_req, res) => res.json(publicPositions()));

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

async function shutdown() {
  isStopping = true;
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (marketReconnectTimer) clearTimeout(marketReconnectTimer);
  if (snapshotRetryTimer) clearTimeout(snapshotRetryTimer);
  if (userSocket) userSocket.close();
  if (marketSocket) marketSocket.close();
  try {
    if (listenKey && API_KEY) await apiKeyRequest("DELETE", "/fapi/v1/listenKey");
  } catch (err) {
    console.error("關閉 listenKey 時發生錯誤：", err.message);
  }
  httpServer.close(() => process.exit(0));
}
