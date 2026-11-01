require("dotenv").config();

const express = require("express");
const crypto = require("crypto");
const WebSocket = require("ws");
const path = require("path");

const app = express();
const PORT = Number(process.env.PORT || 6092);
const API_KEY = process.env.BINANCE_API_KEY || "";
const API_SECRET = process.env.BINANCE_API_SECRET || "";
const REST_BASE = (process.env.BINANCE_REST_BASE || "https://fapi.binance.com").replace(/\/$/, "");
const WS_BASE = (process.env.BINANCE_WS_BASE || "wss://fstream.binance.com/private").replace(/\/$/, "");
const BTCUSDC_ONLY = String(process.env.BTCUSDC_ONLY || "false").toLowerCase() === "true";

app.use(express.static(path.join(__dirname, "public")));
app.locals.clients = new Set();

let listenKey = null;
let userSocket = null;
let keepAliveTimer = null;
let reconnectTimer = null;
let reconnectDelay = 3000;
let isStopping = false;
let lastEventAt = null;
let connectionState = "啟動中";
let lastError = null;
const positions = new Map();

function positionKey(symbol, side) {
  return `${symbol}:${side || "BOTH"}`;
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

function publicPositions() {
  return [...positions.values()]
    .filter(p => isAllowedSymbol(p.symbol))
    .sort((a, b) => a.symbol.localeCompare(b.symbol) || a.positionSide.localeCompare(b.positionSide));
}

function publishPositions() {
  sendToBrowsers("positions", publicPositions());
}

async function signedRequest(method, endpoint, params = {}) {
  const query = new URLSearchParams({ ...params, timestamp: String(Date.now()), recvWindow: "10000" });
  const signature = crypto.createHmac("sha256", API_SECRET).update(query.toString()).digest("hex");
  const response = await fetch(`${REST_BASE}${endpoint}?${query.toString()}&signature=${signature}`, {
    method,
    headers: { "X-MBX-APIKEY": API_KEY }
  });
  const text = await response.text();
  let data;
  try { data = JSON.parse(text); } catch { throw new Error(`Binance 回傳非 JSON：HTTP ${response.status}`); }
  if (!response.ok) throw new Error(`Binance API ${data.code ?? response.status}: ${data.msg || response.statusText}`);
  return data;
}

async function apiKeyRequest(method, endpoint) {
  const response = await fetch(`${REST_BASE}${endpoint}`, {
    method,
    headers: { "X-MBX-APIKEY": API_KEY }
  });
  const text = await response.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch {}
  if (!response.ok) throw new Error(`listenKey API ${data.code ?? response.status}: ${data.msg || response.statusText}`);
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
      isolatedMargin: String(p.iw ?? old.isolatedMargin ?? "0"),
      source: "WebSocket 更新"
    });
  }
  lastEventAt = Date.now();
  publishPositions();
}

async function loadInitialSnapshot() {
  const data = await signedRequest("GET", "/fapi/v3/positionRisk");
  positions.clear();
  for (const raw of data) {
    const p = mapRestPosition(raw);
    if (Number(p.positionAmt) !== 0) positions.set(positionKey(p.symbol, p.positionSide), p);
  }
  publishPositions();
  console.log(`已取得初始持倉快照：${positions.size} 筆非零持倉`);
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
  connectionState = "重新連線中";
  sendToBrowsers("status", { state: connectionState, error: lastError });
  reconnectTimer = setTimeout(async () => {
    reconnectTimer = null;
    try { await connectUserStream(); }
    catch (err) {
      lastError = err.message;
      console.error("重新連線失敗：", err.message);
      scheduleReconnect();
    }
  }, reconnectDelay);
  reconnectDelay = Math.min(Math.round(reconnectDelay * 1.7), 30000);
}

function connectSocket(key) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`${WS_BASE}/ws/${key}`);
    userSocket = socket;
    let opened = false;
    const timeout = setTimeout(() => {
      if (!opened) {
        socket.terminate();
        reject(new Error("WebSocket 連線逾時"));
      }
    }, 15000);

    socket.on("open", () => {
      opened = true;
      clearTimeout(timeout);
      reconnectDelay = 3000;
      connectionState = "已連線";
      lastError = null;
      console.log("Binance User Data Stream 已連線");
      sendToBrowsers("status", { state: connectionState, error: null });
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
          symbol: event.o?.s, side: event.o?.S, status: event.o?.X,
          executionType: event.o?.x, quantity: event.o?.q,
          averagePrice: event.o?.ap, eventTime: event.E
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
        connectionState = "已斷線";
        sendToBrowsers("status", { state: connectionState, error: lastError });
        scheduleReconnect();
      }
    });
  });
}

async function connectUserStream() {
  connectionState = "取得 listenKey";
  sendToBrowsers("status", { state: connectionState, error: null });
  await createListenKey();
  await connectSocket(listenKey);
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  keepAliveTimer = setInterval(() => keepAlive().catch(err => {
    lastError = err.message;
    console.error(err.message);
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
  console.log(`本機持倉監控頁面：http://localhost:${PORT}`);
  if (!API_KEY || !API_SECRET) {
    connectionState = "尚未設定 .env";
    console.log("請複製 .env.example 為 .env，並填入 API Key / Secret。");
    return;
  }
  try {
    await loadInitialSnapshot();
    await connectUserStream();
  } catch (err) {
    lastError = err.message;
    connectionState = "連線失敗";
    console.error("啟動失敗：", err.message);
    sendToBrowsers("status", { state: connectionState, error: lastError });
    scheduleReconnect();
  }
});

const browserWss = new WebSocket.Server({ server: httpServer, path: "/browser-ws" });
browserWss.on("connection", client => {
  app.locals.clients.add(client);
  client.send(JSON.stringify({ type: "status", payload: { state: connectionState, error: lastError }, serverTime: Date.now() }));
  client.send(JSON.stringify({ type: "positions", payload: publicPositions(), serverTime: Date.now() }));
  client.on("close", () => app.locals.clients.delete(client));
});

app.get("/api/status", (_req, res) => res.json({
  state: connectionState, lastEventAt, positionCount: publicPositions().length, error: lastError
}));
app.get("/api/positions", (_req, res) => res.json(publicPositions()));

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

async function shutdown() {
  isStopping = true;
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (userSocket) userSocket.close();
  try {
    if (listenKey && API_KEY) await apiKeyRequest("DELETE", "/fapi/v1/listenKey");
  } catch (err) {
    console.error("關閉 listenKey 時發生錯誤：", err.message);
  }
  httpServer.close(() => process.exit(0));
}
