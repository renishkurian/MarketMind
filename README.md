# MarketMind 📈

**Institutional-Grade Market Intelligence & Portfolio Management**

MarketMind is a high-performance stock analysis platform designed to bridge the gap between technical indicators, fundamental health, and AI-driven insights. It provides a clean, premium dashboard for tracking portfolios with institutional-level precision.

---

## 🚀 Key Features

### 1. Hybrid Intelligence Engine (V2.1)
- **Scoring System**: Comprehensive 0-100 scoring across four dimensions:
    - **Fundamental**: PE/5Y, ROE, Debt/Equity, and Growth CAGR.
    - **Technical**: RSI, MACD, SMA 50/200, ADX, and Bollinger Bands.
    - **Momentum**: 1Y/60D/20D ROC and Relative Strength vs NIFTY.
    - **Sector Rank**: Percentile ranking against industry peers.
- **Data Confidence**: Real-time visibility into data completeness (FULL vs. TECHNICALS_ONLY).

### 2. ✨ AI Graph Reader (Deep Dive)
- **Interactive Chat**: Ask the AI to analyze specific chart patterns or technical setups.
- **Dynamic Trendlines**: The AI can dynamically plot Support/Resistance trendlines directly onto the interactive canvas.
- **Multi-Session History**: Persistently store and manage multiple chat sessions per stock for longitudinal research.

### 3. Portfolio & Signal Sync
- **Bulk Synchronization**: One-click recomputation of signals for your entire portfolio to ensure your data is never stale.
- **Tax-Lot Tracking**: Real-time profit/loss tracking with detailed tax-lot management.
- **Institutional Quality**: Automated data fetching from official exchange archives and Yahoo Finance.

---

## 🛠️ Tech Stack

- **Frontend**: React, Lucide Icons, Lightweight-Charts (Canvas), TailwindCSS.
- **Backend**: FastAPI (Python), SQLAlchemy, MySQL (Production) / SQLite (Development).
- **AI**: Integrations for Claude (Anthropic), GPT-4 (OpenAI), and Grok-Beta (xAI).
- **Data**: Custom EOD Archive parsers and Yahoo Finance Sync.

---

## 🏁 Getting Started

### Prerequisites
- Python 3.12+
- Node.js 18+
- MySQL Server

### Backend Setup
1. Create a virtual environment: `python -m venv .venv`
2. Install dependencies: `pip install -r requirements.txt`
3. Configure `.env` with your API keys and DB credentials.
4. Run the API: `uvicorn backend.main:app --reload`

### Frontend Setup
1. Install dependencies: `npm install`
2. Start the dev server: `npm run dev`

---

## ⚖️ Disclaimer
*For informational purposes only. Not financial advice.*
