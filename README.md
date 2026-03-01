# 📈 Dalal Street Intelligence — NSE/BSE Stock Analyzer

A full-stack, AI-powered stock analysis platform for Indian markets (NSE & BSE).

![Tech Stack](https://img.shields.io/badge/Python-FastAPI-009688?style=flat-square) ![Ollama](https://img.shields.io/badge/AI-Ollama-7c3aed?style=flat-square) ![DB](https://img.shields.io/badge/DB-SQLite-003B57?style=flat-square)

---

## 🚀 Features

### Fundamental Analysis
- P/E, P/B, EV/EBITDA, P/S ratios
- ROE, ROA, Profit/Operating/Gross margins
- Revenue & earnings growth (YoY, quarterly)
- Debt-to-equity, current ratio, quick ratio, free cash flow
- EPS (trailing & forward), book value, dividend analysis
- Analyst consensus with price targets

### Technical Analysis
- **Moving Averages**: SMA 20/50/200, EMA 12/26/50/200, Golden/Death cross
- **Momentum**: RSI (14), MACD + signal + histogram, Stochastic %K/%D
- **Volatility**: Bollinger Bands (%B, Bandwidth), ATR (14)
- **Trend**: ADX + DI+/DI-, OBV trend
- **Volume**: 20-day average, ratio analysis
- **Levels**: Support/Resistance, Pivot Points (R1/R2/R3, S1/S2/S3)
- **Overall Signal**: Weighted aggregation → STRONG BUY / BUY / NEUTRAL / SELL / STRONG SELL

### AI Analysis (via Ollama)
- Fundamental deep-dive report
- Technical trade setup with entry/stop-loss/targets
- Combined investment thesis (BUY/HOLD/SELL with reasoning)
- All analysis stored in SQLite for history

### Other Features
- Live price data via yfinance (NSE & BSE)
- Watchlist with live price tracking
- Interactive charts (price, volume, RSI)
- SQLite caching (reduces API calls)
- 20 popular Nifty 50 stocks pre-loaded

---

## 🛠️ Setup

### 1. Prerequisites
```bash
Python 3.10+
Ollama installed and running
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Setup Ollama
```bash
# Start Ollama on port 11435 (non-default port)
OLLAMA_HOST=0.0.0.0:11435 ollama serve

# In another terminal, pull a model
ollama pull llama3.2         # Recommended (4GB)
# OR
ollama pull mistral          # Alternative (4GB)
# OR
ollama pull qwen2.5          # Great for finance (4GB)
```

### 4. Run the App
```bash
cd stock-analyzer
python run.py
```

Open your browser at: **http://localhost:8000**

---

## 📁 Project Structure

```
stock-analyzer/
├── backend/
│   ├── __init__.py
│   ├── main.py          # FastAPI app & all API routes
│   ├── database.py      # SQLite setup, caching, watchlist
│   ├── stock_data.py    # yfinance data fetching (NSE/BSE)
│   ├── technicals.py    # All technical indicators
│   └── ai_analysis.py  # Ollama AI prompts & integration
├── frontend/
│   └── index.html       # Single-page app (Tailwind + Chart.js)
├── run.py               # Uvicorn startup script
├── requirements.txt
└── stock_analyzer.db    # Auto-created SQLite database
```

---

## 🔌 API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/stock/{exchange}/{symbol}` | Stock info & price |
| GET | `/api/stock/{exchange}/{symbol}/fundamentals` | Full fundamental data |
| GET | `/api/stock/{exchange}/{symbol}/technicals` | All technical indicators |
| GET | `/api/stock/{exchange}/{symbol}/history?period=1y` | OHLCV history |
| POST | `/api/analyze/ai` | Run Ollama AI analysis |
| GET | `/api/watchlist` | Get watchlist with live prices |
| POST | `/api/watchlist` | Add to watchlist |
| DELETE | `/api/watchlist/{exchange}/{symbol}` | Remove from watchlist |
| GET | `/api/models` | Available Ollama models |
| GET | `/api/popular` | Pre-loaded popular stocks |

---

## 📊 How to Use

1. **Search a stock**: Type any NSE/BSE symbol (e.g. `RELIANCE`, `TCS`, `HDFCBANK`)
2. **Overview tab**: Quick snapshot of price, signals, key ratios
3. **Fundamentals tab**: Full valuation, profitability, growth, health metrics
4. **Technicals tab**: All indicators with buy/sell signals and support/resistance levels
5. **Chart tab**: Interactive price chart with moving averages, volume, RSI
6. **AI Analysis tab**: Select model type + Ollama model → get LLM-powered analysis

---

## ⚡ NSE Symbols

Append `.NS` for NSE (handled automatically), `.BO` for BSE.

Common symbols: `RELIANCE`, `TCS`, `HDFCBANK`, `INFY`, `ICICIBANK`, `SBIN`, `WIPRO`, `BHARTIARTL`, `ITC`, `HINDUNILVR`, `LT`, `AXISBANK`, `KOTAKBANK`, `BAJFINANCE`, `TITAN`, `MARUTI`, `NESTLEIND`, `ASIANPAINT`, `SUNPHARMA`, `TATAMOTORS`

---

## 🧠 AI Models Recommended

| Model | Size | Best For |
|-------|------|----------|
| `llama3.2` | 2GB | Fast analysis, good quality |
| `llama3.1` | 4GB | Detailed, nuanced reports |
| `qwen2.5` | 4GB | Excellent financial reasoning |
| `mistral` | 4GB | Good overall |
| `deepseek-r1` | varies | Deep reasoning, slower |

---

## 🔧 Configuration

To change Ollama port, edit `backend/ai_analysis.py`:
```python
OLLAMA_BASE_URL = "http://localhost:11435"  # Change if needed
```

---

## ⚠️ Disclaimer

This tool is for **educational and research purposes only**. Do not make investment decisions solely based on this analysis. Always consult a SEBI-registered financial advisor before investing.
