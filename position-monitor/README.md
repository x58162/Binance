# Binance USDⓈ-M Futures Position Monitor（唯讀）

Node.js 本機持倉監控範例：先透過 REST API 取得初始完整持倉快照，再透過 User Data Stream WebSocket 接收變更。

## 功能
- 顯示目前非零持倉、交易對、方向、數量、開倉均價、標記價格與未實現盈虧
- 顯示 REST 回傳的強平價格、槓桿、保證金模式與逐倉保證金
- 接收 ACCOUNT_UPDATE 與 ORDER_TRADE_UPDATE
- 每 30 分鐘延長 listenKey
- WebSocket 斷線後自動重連
- 本機儀表板：http://localhost:6092

## 安全設定
1. 建立專用 Binance API Key。
2. **只開啟讀取權限**；不要開啟合約交易、現貨交易、提領或轉帳權限。
3. 不要把 .env 傳給別人或上傳 GitHub。
4. 此程式沒有下單、撤單、平倉、槓桿修改或轉帳功能。

## 安裝
需要 Node.js 18 或更新版本。

```bash
npm install
```

Windows CMD：
```bat
copy .env.example .env
```

PowerShell：
```powershell
Copy-Item .env.example .env
```

編輯 .env：
```env
BINANCE_API_KEY=你的_API_KEY
BINANCE_API_SECRET=你的_API_SECRET
BINANCE_REST_BASE=https://fapi.binance.com
BINANCE_WS_BASE=wss://fstream.binance.com/private
PORT=6092
BTCUSDC_ONLY=false
```

啟動：
```bash
npm start
```

開啟 http://localhost:6092

## API 路徑
- 初始持倉快照：GET /fapi/v3/positionRisk（SIGNED USER_DATA）
- 建立串流：POST /fapi/v1/listenKey
- 延長串流：PUT /fapi/v1/listenKey
- WebSocket：wss://fstream.binance.com/private/ws/<listenKey>
- 持倉事件：ACCOUNT_UPDATE
- 訂單事件：ORDER_TRADE_UPDATE

ACCOUNT_UPDATE 是增量事件；程式先讀取 REST 快照，再用事件更新快取。事件不一定包含標記價格、強平價格或槓桿，因此這些欄位會保留 REST 快照值，需要額外資料來源才能持續更新。

## BTCUSDC 篩選
將 BTCUSDC_ONLY 設為 true 即可只顯示 BTCUSDC。

## 注意
- 不要把本機服務直接暴露到公網。
- 若收到 -2015，檢查 API Key 權限、IP 白名單與帳戶 API 設定。
- 若收到 -1021，檢查電腦系統時間。
- 此為教學用唯讀範例，不保證零延遲。
