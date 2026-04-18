# MarketMind

MarketMind is an Indian Stock Market Portfolio Intelligence App built as a full-stack dashboard utilizing real-time live price updates, technical + fundamental scoring, and AI narrative insights using Anthropic's Claude.

## Features
- **Real-time Engine**: Fast WebSocket-based updates feeding directly to a responsive React UI.
- **Scoring Engine**: Evaluates MACD, RSI, SMAs, Bollinger Bands, Volume, PE ratios, ROE, Debt/Equity.
- **NSE Bhavcopy**: Official EOD data pulled directly from the NSE archives.
- **AI Narrative**: Custom Claude-generated stock performance snapshots and summaries.
- **Hardware Agnostic**: Fully Dockerized to easily run on a Raspberry Pi 4.

## Local Development Setup

**Prerequisites**: Python 3.11+, Node 18+, MySQL 8.0

1. Clone and navigate to the project directory.
2. Backend Setup:
   ```bash
   cd backend
   pip install -r requirements.txt
   cp .env.example .env # Fill in values
   ```
3. Initialize the database and history:
   ```bash
   cd ..
   python scripts/init_db.py
   python scripts/load_historical.py
   # Optional: python scripts/verify_symbols.py
   ```
4. Start the Application:
   - **Terminal 1 (Backend API)**: `uvicorn backend.main:app --reload`
   - **Terminal 2 (Scheduler)**: `python backend/scheduler.py`
   - **Terminal 3 (Frontend)**:
     ```bash
     cd frontend
     npm install
     cp .env.example .env
     npm run dev
     ```
5. Open your browser and navigate to `http://localhost:5173`.

## Raspberry Pi Deployment (Docker)

**Prerequisites**: Docker + docker-compose installed on your Pi.

1. Clone this repository on your Pi.
2. Populate the `.env` secrets.
3. Run the complete stack:
   ```bash
   docker-compose up -d --build
   ```
4. Run the database initialization inside the backend container:
   ```bash
   docker exec -it marketmind_backend python scripts/init_db.py
   docker exec -it marketmind_backend python scripts/load_historical.py
   ```
5. Access your Raspberry Pi's IP address on port 80 to view the Dashboard.
