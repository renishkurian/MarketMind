"""
MarketMind — AI Engine (V2)
============================
Routes insight generation to the correct LLM provider and writes a full
audit record to `ai_call_logs` for every API call made.

Logged per call:
  - provider + model name
  - prompt tokens, completion tokens, total tokens
  - wall-clock duration (ms)
  - full request payload (messages list)
  - structured JSON response
  - status (SUCCESS / ERROR) + error_message
"""

import openai
import json
import logging
import time
import os
from datetime import datetime
from typing import Optional

import anthropic
import pandas as pd

from backend.config import settings
from backend.data.db import SessionLocal, AICallLog, SystemConfig

logger = logging.getLogger(__name__)


async def _get_ai_settings() -> dict:
    """Load AI settings from system_config DB, fallback to in-memory."""
    try:
        async with SessionLocal() as session:
            from sqlalchemy import select
            result = await session.execute(select(SystemConfig))
            db_cfg = {row.key: row.value for row in result.scalars().all()}
        return {
            "provider": db_cfg.get("AI_PROVIDER", settings.AI_PROVIDER).lower(),
            "openai_key": db_cfg.get("OPENAI_API_KEY", settings.OPENAI_API_KEY),
            "openai_model": db_cfg.get("OPENAI_MODEL", settings.OPENAI_MODEL) or "gpt-4o",
            "anthropic_key": db_cfg.get("ANTHROPIC_API_KEY", settings.ANTHROPIC_API_KEY),
            "anthropic_model": db_cfg.get("ANTHROPIC_MODEL", settings.ANTHROPIC_MODEL) or "claude-sonnet-4-5",
            "xai_key": db_cfg.get("XAI_API_KEY", settings.XAI_API_KEY),
            "xai_model": db_cfg.get("XAI_MODEL", settings.XAI_MODEL) or "grok-beta",
        }
    except Exception as e:
        logger.warning(f"Failed to read AI settings from DB, using in-memory: {e}")
        return {
            "provider": settings.AI_PROVIDER.lower(),
            "openai_key": settings.OPENAI_API_KEY,
            "openai_model": settings.OPENAI_MODEL or "gpt-4o",
            "anthropic_key": settings.ANTHROPIC_API_KEY,
            "anthropic_model": settings.ANTHROPIC_MODEL or "claude-sonnet-4-5",
            "xai_key": settings.XAI_API_KEY,
            "xai_model": settings.XAI_MODEL or "grok-beta",
        }

# ── Helpers ───────────────────────────────────────────────────────────────────

async def _write_call_log(
    symbol: str,
    provider: str,
    model: str,
    trigger_reason: str,
    skill_id: Optional[str],
    messages: list,
    response_dict: Optional[dict],
    prompt_tokens: int,
    completion_tokens: int,
    duration_ms: int,
    status: str = "SUCCESS",
    error_message: Optional[str] = None,
    insight_id: Optional[int] = None,
    user_id: Optional[int] = None,
) -> None:
    """Persist a log entry to ai_call_logs."""
    try:
        async with SessionLocal() as session:
            entry = AICallLog(
                insight_id=insight_id,
                symbol=symbol,
                skill_id=skill_id,
                provider=provider,
                model=model,
                trigger_reason=trigger_reason,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=prompt_tokens + completion_tokens,
                duration_ms=duration_ms,
                status=status,
                error_message=error_message,
                request_payload=messages,
                response_raw=response_dict,
                called_at=datetime.utcnow(),
                user_id=user_id,
            )
            session.add(entry)
            await session.commit()
    except Exception as e:
        logger.error(f"Failed to write AI call log: {e}")


def _build_messages(prompt: str) -> list:
    return [
        {"role": "system", "content": "You are a financial analysis assistant. Always respond with valid JSON only — no text outside the JSON object."},
        {"role": "user", "content": prompt},
    ]


# ── Helpers ───────────────────────────────────────────────────────────────────

# Removed legacy _build_prompt in favor of SkillLoader for rich md templates

def _build_fallback_prompt(
    symbol: str,
    company_name: str,
    trigger_reason: str,
    signals: dict,
    fundamentals: dict,
) -> str:
    """
    Rich fallback used when no skill_id matches.
    Passes composite score, all FA metrics, and signals context
    so the AI produces meaningful analysis even without a skill template.
    """
    comp   = signals.get("composite_score", "N/A")
    fa_s   = signals.get("fundamental_score", "N/A")
    ta_s   = signals.get("technical_score", "N/A")
    mom_s  = signals.get("momentum_score", "N/A")
    pctile = signals.get("sector_percentile", "N/A")
    st_sig = signals.get("st_signal", "HOLD")
    lt_sig = signals.get("lt_signal", "HOLD")
    price  = signals.get("current_price", "N/A")

    pe      = fundamentals.get("pe_ratio", "N/A")
    roe     = fundamentals.get("roe", "N/A")
    de      = fundamentals.get("debt_equity", "N/A")
    rev_g   = fundamentals.get("revenue_growth_3yr", "N/A")
    pat_g   = fundamentals.get("pat_growth_3yr", "N/A")
    margin  = fundamentals.get("operating_margin", "N/A")
    pledge  = fundamentals.get("promoter_pledge_pct", "N/A")
    sector  = signals.get("sector") or fundamentals.get("sector", "Unknown")

    return f"""You are a senior equity analyst at an institutional fund focused on Indian markets (NSE/BSE).

STOCK: {company_name} ({symbol}) | SECTOR: {sector} | PRICE: ₹{price}
TRIGGER: {trigger_reason}

SCORING ENGINE OUTPUT (MarketMind v2.1):
  Composite Score    : {comp}/100
  Fundamental Score  : {fa_s}/100  |  Technical: {ta_s}/100  |  Momentum: {mom_s}/100
  Sector Percentile  : {pctile}th (beats this % of sector peers)
  ST Signal: {st_sig}  |  LT Signal: {lt_sig}

KEY FUNDAMENTALS:
  PE Ratio: {pe}  |  ROE: {roe}%  |  Debt/Equity: {de}
  Revenue CAGR (3yr): {rev_g}%  |  PAT CAGR (3yr): {pat_g}%
  Operating Margin: {margin}%  |  Promoter Pledge: {pledge}%

ANALYSIS INSTRUCTIONS:
1. Evaluate the scoring engine output — is the composite score justified by the fundamentals?
2. Identify the single biggest opportunity and single biggest risk for a 3-year horizon.
3. Give a clear verdict: BUY / HOLD / SELL with one specific reason.
4. Flag the promoter pledge if above 20% as a governance risk.
5. Reference India-specific context (sector tailwinds, regulatory environment, GST/formalization).

Return ONLY valid JSON — no markdown outside the reply field:
{{
    "short_summary": "2–3 sentence actionable summary with verdict and key reason.",
    "long_summary": "3–4 paragraph analysis: scoring context, fundamental quality, key risk, India macro angle.",
    "verdict": "BUY or HOLD or SELL",
    "key_risks": ["specific risk 1", "specific risk 2"],
    "key_opportunities": ["specific opportunity 1", "specific opportunity 2"],
    "sentiment_score": 0.0
}}
Note: sentiment_score is 0.0 (most bearish) to 1.0 (most bullish).
"""


def build_chart_chat_system_prompt(symbol: str, context_data: dict) -> str:
    """
    Clean, structured system prompt for the chart ASK feature.
    Passes composite score upfront. Separates data sections clearly.
    No placeholder text in the output schema.
    """
    import json

    today     = context_data.get("today", {})
    summary   = context_data.get("price_summary", {})
    signals   = context_data.get("indicators", {})
    news      = context_data.get("recent_news", [])
    comp      = context_data.get("composite_score", "N/A")
    st_sig    = context_data.get("current_st_signal", "HOLD")
    lt_sig    = context_data.get("current_lt_signal", "HOLD")
    bt        = context_data.get("backtest", {})
    bt_str    = (f"{bt.get('cagr', 'N/A')}% CAGR, {bt.get('win_rate', 'N/A')}% Win Rate, {bt.get('sharpe', 'N/A')} Sharpe" 
                 if bt.get('cagr') else "Not available")

    news_block = "\n".join(f"• {h}" for h in news[:5]) if news else "No recent news found."

    recent_bars = summary.get("recent_5_bars", [])
    bars_block  = json.dumps(recent_bars, indent=2) if recent_bars else "Not available."

    weekly = summary.get("weekly_candles", [])
    weekly_block = json.dumps(weekly[-6:], indent=2) if weekly else "Not available."

    return f"""You are MarketMind's institutional chart analyst for NSE/BSE Indian equities.
Your role is to give precise, data-grounded technical analysis backed by live news context.

═══ STOCK OVERVIEW ═══
Symbol          : {symbol}
Composite Score : {comp}/100
ST Signal       : {st_sig}  |  LT Signal: {lt_sig}
Backtest (2016+) : {bt_str}

═══ TODAY'S VERIFIED INTRADAY DATA ═══
Use these EXACT figures when discussing today's price action. Do not estimate.
Date   : {today.get('date', 'N/A')}
Open   : ₹{today.get('open', 'N/A')}
High   : ₹{today.get('high', 'N/A')}
Low    : ₹{today.get('low', 'N/A')}
Close  : ₹{today.get('close', 'N/A')}  (prev close: ₹{today.get('prev_close', 'N/A')})
Change : {today.get('change_pct', 'N/A')}%
Volume : {today.get('volume', 'N/A')}

═══ 90-DAY PRICE SUMMARY ═══
Period change   : {summary.get('period_change_pct', 'N/A')}%  ({summary.get('period_start_close')} → {summary.get('period_end_close')})
90d High / Low  : ₹{summary.get('90d_high')} / ₹{summary.get('90d_low')}
Avg Daily Volume: {summary.get('avg_volume_90d')}
SMA 20 / 50 / 90: ₹{summary.get('sma_20')} / ₹{summary.get('sma_50')} / ₹{summary.get('sma_90')}

Last 5 Daily Bars:
{bars_block}

Weekly Candle Summary (last 6 weeks):
{weekly_block}

═══ CRITICAL CONSTRAINTS — READ BEFORE GENERATING REPLY ═══
CURRENT CLOSE: ₹{today.get('close', 'N/A')}
90D LOW: ₹{summary.get('90d_low', 'N/A')}

RSI VALUE: {signals.get('ta', {}).get('rsi', 'N/A')}
RSI RULE (NON-NEGOTIABLE):
{"⚠ RSI IS BELOW 30 — THIS IS OVERSOLD, NOT OVERBOUGHT. You MUST say 'recovering from oversold'. Saying 'overbought' when RSI<30 is factually wrong." if isinstance(signals.get('ta', {}).get('rsi'), (int, float)) and signals.get('ta', {}).get('rsi', 100) < 30 else
 "⚠ RSI IS 30–45 — recovering from lows. Do NOT say overbought." if isinstance(signals.get('ta', {}).get('rsi'), (int, float)) and signals.get('ta', {}).get('rsi', 100) < 45 else
 "RSI is neutral-to-bullish. Only say overbought if RSI > 70."}

ENTRY RULE (NON-NEGOTIABLE):
Current close is ₹{today.get('close', 'N/A')}. Do NOT suggest entry below ₹{round(float(today.get('close', 0)) * 0.92, 2) if today.get('close') else 'N/A'} (more than 8% below current price) unless you explicitly justify a breakdown scenario. The 90d low of ₹{summary.get('90d_low', 'N/A')} is historical — do not use it as a default entry target if price has already recovered from it.

═══ INDICATOR SIGNALS ═══
{json.dumps(signals, indent=2)}

═══ LIVE NEWS (Google News) ═══
Use these to explain fundamental triggers behind price moves.
If no news matches the move, say "appears purely technical."
{news_block}

═══ RESPONSE RULES ═══
1. Reply ONLY as valid JSON — no text outside the JSON object.
2. Always cite today's verified High/Low when discussing intraday action.
3. Identify support/resistance from weekly candles and SMA levels.
4. State your buy/hold/sell conviction with a specific reason.
4a. ENTRY POINT RULE: Never suggest an entry price significantly below the current close unless there is a confirmed breakdown. If price has already bounced from lows, the realistic entry is near current price or a minor pullback (5–10%), not a re-test of the prior low.
4b. EXIT / TARGET RULE: Base exit/target on actual SMA levels and resistance from weekly candles in the data above — do not fabricate levels not present in the data.
5. Only include trend_lines that directly answer the question asked.
5a. CRITICAL: trend_line start_price and end_price MUST equal the exact price level named in the reply. If the reply says support is at ₹143.13, the line must be drawn at 143.13 — never at the current price.
6. Format all dates as YYYY-MM-DD matching the data above.
7. When computing price_target, use backtest CAGR to project forward and win_rate to set band width.
   A 55% win rate = wider band. A 75% win rate = tighter band. Never fabricate — if data is insufficient return null.

Return this exact structure:
{{
  "reply": "Your markdown-formatted analysis — cite prices, signals, and news. Be precise and direct.",
  "trend_lines": [
    {{
      "start_date": "YYYY-MM-DD",
      "end_date": "YYYY-MM-DD",
      "start_price": 0.0,
      "end_price": 0.0,
      "color": "green",
      "label": "Support"
    }}
  ],
  "price_target": {{
    "target_30d": 0.0,
    "target_90d": 0.0,
    "confidence_low": 0.0,
    "confidence_high": 0.0,
    "horizon": "30d",
    "basis": "One sentence explaining the target rationale"
  }}
}}
Return trend_lines as [] if none apply.
Return price_target as null if the question is not about price direction, entry, exit, or targets.
If the question asks about entry point, exit point, buy zone, sell zone, or target — you MUST populate price_target:
  - confidence_low = the entry / buy zone price
  - confidence_high = the exit / target price
  - basis = one sentence explaining the entry and exit rationale
Use backtest CAGR and win_rate from context to calibrate confidence_low/high band width.
confidence_low and confidence_high are absolute price levels, not percentages.
"""


def build_fundamentals_research_prompt(symbol: str, company_name: str) -> str:
    """
    Grounded fundamentals research prompt.
    """
    from datetime import datetime
    current_year = datetime.now().year

    return f"""You are a senior equity research analyst specialising in NSE/BSE listed Indian companies.

TASK: Research and extract the most recent financial data for: {company_name} ({symbol})

DATA SOURCES TO USE (in priority order):
1. NSE/BSE official exchange filings (quarterly results, annual report)
2. screener.in or tickertape.in (India-focused fundamental databases)
3. Company investor relations website
4. moneycontrol.com or economictimes.com financial data

IMPORTANT: Only use data from FY{current_year-1} or FY{current_year} (April {current_year-1}–March {current_year}).
Do NOT use data older than 2 financial years. If you cannot find recent data, return null.

DECIMAL CONVENTION (CRITICAL):
- All percentage values must be plain numbers: ROE of 18.5% → 18.5, NOT 0.185
- Revenue growth of 12% → 12.0, NOT 0.12
- Operating margin of 20% → 20.0, NOT 0.20
- This applies to ALL percentage fields without exception.

Return ONLY a valid JSON object with these exact keys:
{{
    "pe_ratio": float or null,
    "eps": float or null,
    "roe": float or null,
    "debt_equity": float or null,
    "revenue_growth": float or null,
    "market_cap": integer (INR) or null,
    "revenue_growth_3yr": float or null,
    "pat_growth_3yr": float or null,
    "operating_margin": float or null,
    "pe_5yr_avg": float or null,
    "roe_3yr_avg": float or null,
    "peg_ratio": float or null,
    "pb_ratio": float or null,
    "ev_ebitda": float or null,
    "held_percent_institutions": float or null,
    "promoter_holding": float or null,
    "promoter_pledge_pct": float or null,
    "analyst_rating": float or null,
    "recommendation_key": "strong_buy" or "buy" or "hold" or "underperform" or "sell" or null,
    "total_cash": integer (INR) or null,
    "total_debt": integer (INR) or null,
    "current_ratio": float or null,
    "data_confidence": "HIGH" or "MEDIUM" or "LOW",
    "data_source": "brief description of where data was found",
    "data_as_of": "FY2024 Q3" or similar period label
}}

Return null for any metric you cannot find with confidence. Never invent numbers.
data_confidence should reflect: HIGH = verified from official filing, MEDIUM = from financial portal,
LOW = estimated or older than 1 financial year.
"""


def build_portfolio_allocation_prompt(amount: float, portfolio_summary: str) -> str:
    """
    Portfolio allocation prompt with India-specific guardrails.
    """
    return f"""You are a SEBI-registered portfolio manager allocating fresh capital for an Indian equity investor.

ALLOCATION TASK:
Distribute exactly ₹{amount:,.0f} across the portfolio below.
The sum of all allocated_amount values MUST equal exactly {amount}.

PORTFOLIO STATE:
{portfolio_summary}

ALLOCATION RULES:
1. Weight higher toward stocks with composite_score > 65 and BUY signals.
2. SECTOR CONCENTRATION: No single sector may receive more than 40% of the total.
3. MINIMUM ALLOCATION: No stock receives less than ₹{max(500, amount * 0.03):,.0f}
   (3% floor) — avoid meaningless micro-allocations.
4. LIQUIDITY DISCOUNT: For SMALL-cap stocks, apply a 15% haircut to their 
   score weight to account for liquidity risk.
5. RISK-FREE CONTEXT: India 10-year gilt yields ~6.5%. Only allocate meaningfully 
   to stocks where the composite score suggests potential to beat this hurdle.
6. If any stock has promoter pledge > 30%, limit its allocation to max 10% of total.

ROUNDING: After computing weights, adjust the largest allocation up or down
by a few rupees to ensure the total sums to exactly {amount}.

Return ONLY valid JSON:
{{
  "rationale": "2–3 sentence strategy: sector distribution rationale, score-based weighting logic, key risks flagged.",
  "total_allocated": {amount},
  "allocations": [
    {{
      "symbol": "TICKER",
      "sector": "sector name",
      "composite_score": 0.0,
      "allocated_amount": 0.0,
      "weight_pct": 0.0,
      "estimated_qty": 0,
      "reason": "One specific sentence: why this weight for this stock."
    }}
  ]
}}
"""



# ── Provider calls ────────────────────────────────────────────────────────────

async def _call_openai(messages: list, model: str, api_key: str) -> tuple[dict, int, int]:
    """Returns (parsed_dict, prompt_tokens, completion_tokens)."""
    client = openai.AsyncOpenAI(
        api_key=api_key,
        timeout=120.0,  # 2 min timeout for Raspberry Pi
    )
    resp = await client.chat.completions.create(
        model=model,
        messages=messages,
        response_format={"type": "json_object"},
    )
    parsed = json.loads(resp.choices[0].message.content)
    return parsed, resp.usage.prompt_tokens, resp.usage.completion_tokens


async def _call_openai_text(messages: list, model: str, api_key: str) -> tuple[dict, int, int]:
    """Returns plain text response wrapped as dict (for skill prompts that return markdown, not JSON)."""
    client = openai.AsyncOpenAI(api_key=api_key, timeout=120.0)
    resp = await client.chat.completions.create(model=model, messages=messages)
    text = resp.choices[0].message.content or ""
    return _parse_skill_markdown(text), resp.usage.prompt_tokens, resp.usage.completion_tokens


def _parse_skill_markdown(text: str) -> dict:
    """Extract short_summary, key_risks, key_opportunities from a markdown skill essay."""
    lines = text.strip().splitlines()
    short_summary = ""
    key_risks = []
    key_opportunities = []

    # short_summary = first non-empty, non-heading paragraph
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and not stripped.startswith("---"):
            short_summary = stripped[:300]
            break

    # Extract bullet points under risk/opportunity sections
    in_risk = False
    in_opp = False
    for line in lines:
        lower = line.lower()
        if any(k in lower for k in ["risk", "concern", "red flag", "weakness", "avoid", "warning"]):
            in_risk = True; in_opp = False
        elif any(k in lower for k in ["opportunit", "strength", "moat", "buy", "upside", "catalyst"]):
            in_opp = True; in_risk = False
        stripped = line.strip()
        if stripped.startswith(("- ", "* ", "• ")) or (stripped and stripped[0].isdigit() and ". " in stripped[:4]):
            item = stripped.lstrip("-*•0123456789. ").strip()
            if item and len(item) > 5:
                if in_risk and len(key_risks) < 4:
                    key_risks.append(item)
                elif in_opp and len(key_opportunities) < 4:
                    key_opportunities.append(item)

    # Fallback: if no bullets found, pull verdict line as summary item
    if not key_risks and not key_opportunities:
        for line in lines:
            if "verdict" in line.lower() or "rating" in line.lower():
                item = line.strip().lstrip("#").strip()
                if item:
                    key_opportunities.append(item)
                break

    return {
        "short_summary": short_summary,
        "long_summary": text,
        "key_risks": key_risks,
        "key_opportunities": key_opportunities,
    }


async def _call_anthropic(messages: list, model: str, api_key: str) -> tuple[dict, int, int]:
    client = anthropic.AsyncAnthropic(api_key=api_key)
    resp = await client.messages.create(
        model=model,
        max_tokens=1200,
        messages=messages,
    )
    text = resp.content[0].text
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    parsed = json.loads(text)
    return parsed, resp.usage.input_tokens, resp.usage.output_tokens


async def _call_xai(messages: list, model: str, api_key: str) -> tuple[dict, int, int]:
    client = openai.AsyncOpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1",
        timeout=120.0,
    )
    resp = await client.chat.completions.create(model=model, messages=messages)
    text = resp.choices[0].message.content
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    parsed = json.loads(text)
    return parsed, resp.usage.prompt_tokens, resp.usage.completion_tokens


async def generate_fundamentals(symbol: str, company_name: str = None) -> dict:
    """
    Specialized AI task to research and extract fundamental data for a symbol.
    Returns a dict matching FundamentalsCache columns.
    """
    ai_cfg = await _get_ai_settings()
    provider = ai_cfg["provider"]
    key_map = {
        "openai": ai_cfg["openai_key"],
        "anthropic": ai_cfg["anthropic_key"],
        "xai": ai_cfg["xai_key"],
    }
    
    # Auto-fallback logic (same as generate_insight)
    if not key_map.get(provider):
        for p, k in key_map.items():
            if k:
                provider = p
                break
    
    if not key_map.get(provider):
        raise ValueError(f"No API key configured for AI Fundamentals Research")

    model = {
        "openai": ai_cfg["openai_model"],
        "anthropic": ai_cfg["anthropic_model"],
        "xai": ai_cfg["xai_model"],
    }.get(provider, "gpt-4o")

    company_id = f"{company_name} ({symbol})" if company_name else symbol
    prompt = build_fundamentals_research_prompt(symbol, company_id)
    messages = _build_messages(prompt)
    api_key = key_map[provider]
    
    t0 = time.perf_counter()
    try:
        if provider == "openai":
            parsed, p_tokens, c_tokens = await _call_openai(messages, model, api_key)
        elif provider == "anthropic":
            parsed, p_tokens, c_tokens = await _call_anthropic(messages, model, api_key)
        elif provider == "xai":
            parsed, p_tokens, c_tokens = await _call_xai(messages, model, api_key)
        else:
            raise ValueError(f"Unsupported provider: {provider}")

        duration_ms = int((time.perf_counter() - t0) * 1000)
        
        # Log the call
        await _write_call_log(
            symbol=symbol,
            provider=provider,
            model=model,
            trigger_reason="FUNDAMENTAL_RESEARCH",
            skill_id="fundamentals",
            messages=messages,
            response_dict=parsed,
            prompt_tokens=p_tokens,
            completion_tokens=c_tokens,
            duration_ms=duration_ms
        )
        return parsed

    except Exception as e:
        logger.error(f"Fundamental research failed for {symbol}: {e}")
        raise e


# ── Public entry point ────────────────────────────────────────────────────────

async def generate_insight(
    symbol: str,
    price_history_df: pd.DataFrame,
    signals: dict,
    fundamentals: dict,
    trigger_reason: str,
    skill_id: Optional[str] = None,
    company_name: Optional[str] = None,
    user_id: Optional[int] = None,
) -> dict:
    """
    Generates an AI insight, logs every call to ai_call_logs, and returns
    the structured insight dict.
    """
    # Read latest AI settings from DB
    ai_cfg = await _get_ai_settings()
    provider = ai_cfg["provider"]

    # Check key availability for the selected provider
    key_map = {
        "openai": ai_cfg["openai_key"],
        "anthropic": ai_cfg["anthropic_key"],
        "xai": ai_cfg["xai_key"],
    }
    has_key = bool(key_map.get(provider, ""))

    # Auto-fallback: if selected provider has no key, try others
    if not has_key:
        for fallback_provider, fallback_key in key_map.items():
            if fallback_key:
                logger.info(f"Provider '{provider}' has no key — auto-falling back to '{fallback_provider}'")
                provider = fallback_provider
                has_key = True
                break

    if not has_key:
        logger.warning(f"No API key configured for any provider — returning mock insight.")
        return _get_mock_insight(symbol, trigger_reason, error_msg=f"No API key found for preferred provider '{provider}' and no active fallbacks available.")

    # Resolve model from DB settings
    model = {
        "openai": ai_cfg["openai_model"],
        "anthropic": ai_cfg["anthropic_model"],
        "xai": ai_cfg["xai_model"],
    }.get(provider, "gpt-4o")

    # Load skill and build prompt via SkillLoader
    prompt = None
    if skill_id:
        from backend.engine.consensus.skill_loader import SkillLoader, StockMeta
        from backend.engine.scoring.composite_score import CompositeScoreResult
        
        # Build full StockMeta + CompositeScoreResult mapped from signals dictionary
        meta = StockMeta(
            symbol=symbol,
            isin=signals.get("isin", "N/A"),
            exchange=signals.get("exchange", "NSE"),
            sector=signals.get("sector") or fundamentals.get("sector", "Unknown"),
            market_cap_cr=float(fundamentals.get("market_cap", 0)) / 10_000_000,
            current_price=float(signals.get("current_price", 0)),

            # ── Full FA fields (was only 4, now all 10) ──
            pe_ratio=fundamentals.get("pe_ratio"),
            pe_5yr_avg=fundamentals.get("pe_5yr_avg"),
            roe=fundamentals.get("roe"),
            roe_3yr_avg=fundamentals.get("roe_3yr_avg"),
            debt_equity=fundamentals.get("debt_equity"),
            revenue_growth_3yr=fundamentals.get("revenue_growth_3yr"),
            pat_growth_3yr=fundamentals.get("pat_growth_3yr"),
            operating_margin=fundamentals.get("operating_margin"),
            promoter_holding=fundamentals.get("promoter_holding"),
            promoter_pledge_pct=fundamentals.get("promoter_pledge_pct"),

            # ── Momentum (from signals cache) ──
            roc_252=signals.get("momentum_breakdown", {}).get("roc_1yr"),
            roc_60=signals.get("momentum_breakdown", {}).get("roc_60d"),
            volume_ratio_20_90=signals.get("momentum_breakdown", {}).get("volume_trend"),
        )
        setattr(meta, "company_name", company_name or symbol)

        csr = CompositeScoreResult(
            symbol=symbol,
            isin=meta.isin,
            # ── All 4 component scores, not just 2 ──
            composite_score=float(signals.get("composite_score", 0) or 0),
            fundamental_score=float(signals.get("fundamental_score", 0) or 0),
            technical_score=float(signals.get("technical_score", 0) or 0),
            momentum_score=float(signals.get("momentum_score", 0) or 0),
            sector_rank_score=float(signals.get("sector_rank_score", 0) or 0),
            sector_percentile=float(signals.get("sector_percentile", 50) or 50),
            data_confidence=float(signals.get("data_confidence", 0.5) or 0.5),
            fa_breakdown=signals.get("fa_breakdown") or {},
            ta_breakdown=signals.get("ta_breakdown") or {},
            momentum_breakdown=signals.get("momentum_breakdown") or {},
        )
        
        try:
            loader = SkillLoader()
            prompt = loader.build_prompt(skill_id, meta, csr)
        except Exception as e:
            logger.error(f"Failed to load prompt for skill {skill_id}: {e}")
            
    if not prompt:
        prompt = _build_fallback_prompt(symbol, company_name or symbol, trigger_reason, signals, fundamentals)

    messages = _build_messages(prompt)

    # Call provider
    t0 = time.perf_counter()
    prompt_tokens = completion_tokens = 0
    parsed = None
    status = "SUCCESS"
    error_message = None

    try:
        api_key = key_map[provider]
        if provider == "openai":
            # Skill prompts return markdown essays — use text mode; fallback prompt returns JSON
            if skill_id:
                parsed, prompt_tokens, completion_tokens = await _call_openai_text(messages, model, api_key)
            else:
                parsed, prompt_tokens, completion_tokens = await _call_openai(messages, model, api_key)
        elif provider == "anthropic":
            parsed, prompt_tokens, completion_tokens = await _call_anthropic(messages, model, api_key)
        elif provider == "xai":
            parsed, prompt_tokens, completion_tokens = await _call_xai(messages, model, api_key)
        else:
            return _get_mock_insight(symbol, trigger_reason)

        logger.info(
            f"[AI] {symbol} via {provider}/{model} "
            f"| tokens: {prompt_tokens}+{completion_tokens}={prompt_tokens+completion_tokens} "
            f"| trigger: {trigger_reason}"
        )

    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        logger.error(f"AI call failed for {symbol}: {e}")
        # Provide more context to the mock insight to avoid misleading "No key configured" messages
        parsed = _get_mock_insight(symbol, trigger_reason, error_msg=f"Provider {provider} returned error: {str(e)}")

    duration_ms = int((time.perf_counter() - t0) * 1000)

    # Persist call log
    await _write_call_log(
        symbol=symbol,
        provider=provider,
        model=model,
        trigger_reason=trigger_reason,
        skill_id=skill_id,
        messages=messages,
        response_dict=parsed,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        duration_ms=duration_ms,
        status=status,
        error_message=error_message,
        user_id=user_id,
    )

    return parsed  # ← was missing: caused insights to never be saved to AIInsights table

async def generate_pro_research(
    symbol: str, 
    messages: list, 
    system_prompt: str = "You are an elite institutional trader.",
    trigger_reason: str = "PRO_RESEARCH",
    user_id: Optional[int] = None
) -> dict:
    """
    High-flexibility entry point for deep AI research.
    """
    ai_cfg = await _get_ai_settings()
    provider = ai_cfg["provider"]
    
    # Check key availability for the selected provider
    key_map = {
        "openai": ai_cfg["openai_key"],
        "anthropic": ai_cfg["anthropic_key"],
        "xai": ai_cfg["xai_key"],
    }
    api_key = key_map.get(provider)
    if not api_key:
        for p, k in key_map.items():
            if k:
                provider = p
                api_key = k
                break
    
    t0 = time.perf_counter()
    prompt_tokens = completion_tokens = 0
    res_dict = {}
    status = "SUCCESS"
    error_msg = None

    if not api_key:
        status = "ERROR"
        error_msg = "No API keys configured"
        res_dict = {"reply": "Simulation Mode: No API keys configured in System Settings."}
    else:
        model = {
            "openai": ai_cfg["openai_model"],
            "anthropic": ai_cfg["anthropic_model"],
            "xai": ai_cfg["xai_model"],
        }.get(provider, "gpt-4o")
        
        try:
            full_messages = messages
            if system_prompt:
                 if not any(m.get('role') == 'system' for m in messages):
                     full_messages = [{"role": "system", "content": system_prompt}] + messages

            if provider == "openai":
                res_dict, prompt_tokens, completion_tokens = await _call_openai(full_messages, model, api_key)
            elif provider == "anthropic":
                res_dict, prompt_tokens, completion_tokens = await _call_anthropic(full_messages, model, api_key)
            elif provider == "xai":
                res_dict, prompt_tokens, completion_tokens = await _call_xai(full_messages, model, api_key)
            else:
                raise ValueError(f"Unsupported provider: {provider}")

        except Exception as e:
            status = "ERROR"
            error_msg = str(e)
            logger.error(f"Pro Research AI Error: {e}")
            res_dict = {"reply": f"AI Synthesis Error: {e}"}

    duration_ms = int((time.perf_counter() - t0) * 1000)
    
    await _write_call_log(
        symbol=symbol,
        provider=provider,
        model=model,
        trigger_reason=trigger_reason,
        skill_id="PRO_TIER",
        messages=messages,
        response_dict=res_dict,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        duration_ms=duration_ms,
        status=status,
        error_message=error_msg,
        user_id=user_id
    )
    
    return res_dict


import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import asyncio

def _fetch_news_sync(symbol: str) -> list[str]:
    """Synchronously fetches top news for a symbol using Google News RSS."""
    try:
        query = urllib.parse.quote(f'{symbol} stock India')
        url = f'https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as resp:
            xml_data = resp.read()
            root = ET.fromstring(xml_data)
            headlines = []
            for item in root.findall('.//item')[:5]:
                title = item.find('title').text if item.find('title') is not None else ''
                pubDate = item.find('pubDate').text if item.find('pubDate') is not None else ''
                if title:
                    headlines.append(f'{pubDate}: {title}')
            return headlines
    except Exception as e:
        logger.warning(f"Error fetching news for {symbol}: {e}")
        return []

async def _fetch_symbol_news(symbol: str) -> list[str]:
    return await asyncio.to_thread(_fetch_news_sync, symbol)

async def generate_chart_chat(symbol: str, user_messages: list, context_data: dict, user_id: int = None) -> dict:
    """
    Given a chat history and OHLC/signal context, generate a response containing 
    a markdown 'reply' and an optional array of 'trend_lines'.
    """
    ai_cfg = await _get_ai_settings()
    provider = ai_cfg["provider"]
    key_map = {
        "openai": ai_cfg["openai_key"],
        "anthropic": ai_cfg["anthropic_key"],
        "xai": ai_cfg["xai_key"],
    }
    
    if not key_map.get(provider):
        for p, k in key_map.items():
            if k:
                provider = p
                break
    if not key_map.get(provider):
        raise ValueError(f"No API key configured for AI features.")

    model = {
        "openai": ai_cfg["openai_model"],
        "anthropic": ai_cfg["anthropic_model"],
        "xai": ai_cfg["xai_model"],
    }.get(provider, "gpt-4o")

    # Fetch live news for fundamental context
    recent_news = await _fetch_symbol_news(symbol)
    context_data["recent_news"] = recent_news if recent_news else ["No recent news found."]

    sys_prompt = build_chart_chat_system_prompt(symbol, context_data)

    messages = [{"role": "system", "content": sys_prompt}] + user_messages

    t0 = time.perf_counter()
    status = "SUCCESS"
    error_message = None
    parsed = {}
    prompt_tokens = 0
    completion_tokens = 0

    try:
        api_key = key_map[provider]
        if provider == "openai":
            parsed, prompt_tokens, completion_tokens = await _call_openai(messages, model, api_key)
        elif provider == "anthropic":
            parsed, prompt_tokens, completion_tokens = await _call_anthropic(messages, model, api_key)
        elif provider == "xai":
            parsed, prompt_tokens, completion_tokens = await _call_xai(messages, model, api_key)
            
    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        logger.error(f"Chart chat generation failed ({provider}): {e}")
        raise e

    finally:
        duration_ms = int((time.perf_counter() - t0) * 1000)
        await _write_call_log(
            symbol=symbol,
            provider=provider,
            model=model,
            trigger_reason="CHART_CHAT",
            skill_id=None,
            messages=messages,
            response_dict=parsed,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            duration_ms=duration_ms,
            status=status,
            error_message=error_message,
            user_id=user_id,
        )

    return parsed


async def generate_pattern_recognition(symbol: str, context_data: dict, user_id: int = None) -> dict:
    """
    Silently analyses the last 90 bars and returns any active chart patterns.
    Called automatically on stock page load — no user interaction required.
    Returns: { patterns: [{name, confidence, description, trend_lines}], summary: str }
    """
    ai_cfg = await _get_ai_settings()
    provider = ai_cfg["provider"]
    key_map = {
        "openai":    ai_cfg["openai_key"],
        "anthropic": ai_cfg["anthropic_key"],
        "xai":       ai_cfg["xai_key"],
    }
    if not key_map.get(provider):
        for p, k in key_map.items():
            if k:
                provider = p
                break
    if not key_map.get(provider):
        raise ValueError("No API key configured for AI features.")

    model = {
        "openai":    ai_cfg["openai_model"],
        "anthropic": ai_cfg["anthropic_model"],
        "xai":       ai_cfg["xai_model"],
    }.get(provider, "gpt-4o")

    import json
    summary   = context_data.get("price_summary", {})
    today     = context_data.get("today", {})
    comp      = context_data.get("composite_score", "N/A")
    st_sig    = context_data.get("current_st_signal", "HOLD")
    lt_sig    = context_data.get("current_lt_signal", "HOLD")

    weekly_block = json.dumps(summary.get("weekly_candles", []), indent=2)
    recent_block = json.dumps(summary.get("recent_5_bars", []), indent=2)

    system_prompt = f"""You are an expert technical analyst specialising in NSE/BSE chart pattern detection.
Analyse the provided OHLCV data and identify any ACTIVE chart patterns forming or recently completed.

═══ STOCK DATA ═══
Symbol          : {symbol}
Composite Score : {comp}/100
ST Signal: {st_sig} | LT Signal: {lt_sig}
Current Price   : ₹{today.get('close', 'N/A')}
90d High/Low    : ₹{summary.get('90d_high')} / ₹{summary.get('90d_low')}
SMA 20/50/90    : ₹{summary.get('sma_20')} / ₹{summary.get('sma_50')} / ₹{summary.get('sma_90')}

Weekly Candles (last 18 weeks):
{weekly_block}

Recent 5 Daily Bars:
{recent_block}

═══ PATTERNS TO DETECT ═══
Bullish: Cup & Handle, Inverse Head & Shoulders, Double Bottom, Ascending Triangle,
         Bull Flag, Falling Wedge, Morning Star, Golden Cross,
         V-Bottom Recovery, BB Upper Breakout
Bearish: Head & Shoulders, Double Top, Descending Triangle, Bear Flag,
         Rising Wedge, Evening Star, Death Cross, Rounding Top
Neutral: Symmetrical Triangle, Rectangle, Doji Cluster, Inside Bar

═══ RULES ═══
1. Only report patterns with >= 60% confidence. Return empty array if none qualify.
2. For each pattern provide the key price levels (neckline, target, stop) as trend_lines.
3. confidence is 0.0 to 1.0.
4. implication must be one of: "Bullish", "Bearish", "Neutral".
5. target_price is the measured move target — null if not applicable.
6. Reply ONLY as valid JSON. No text outside the JSON object.

═══ STRICT PATTERN-SPECIFIC CRITERIA ═══
DOUBLE BOTTOM:
  - The two troughs must be within 2% of each other in price. A broad base or gradual
    accumulation zone does NOT qualify as a double bottom.
  - Confidence >= 60% ONLY if price has ALREADY closed above the neckline (the peak
    between the two troughs). If price is still below the neckline, cap confidence at 45%
    (which means it will be excluded by Rule 1).
  - Both troughs must be clearly identifiable on the provided OHLCV bars with distinct
    dates — do not infer a trough from a sideways drift.

DOUBLE TOP:
  - Same mirror logic: two peaks within 2% of each other, and price must have closed
    below the neckline for confidence >= 60%.

HEAD & SHOULDERS / INVERSE HEAD & SHOULDERS:
  - Shoulders must be roughly symmetric in time and price (within 5%).
  - Confidence >= 65% only if neckline break is confirmed or price is within 1% of it.

BULL FLAG / BEAR FLAG:
  - A valid pole (sharp directional move of >= 5% in <= 10 bars) must precede the flag.
  - Flag channel must slope against the pole direction.

CUP & HANDLE:
  - Cup must span at least 6 weeks. Handle must retrace no more than 50% of the cup depth.

V-BOTTOM RECOVERY:
  - Price must have recovered >= 25% from its recent low within <= 15 bars.
  - The low must be a single trough (not a double bottom) — a sharp V shape.
  - Confidence >= 60% only if current price is above both SMA20 and SMA50.
  - Implication: Bullish but flag high reversal risk in the description.

BB UPPER BREAKOUT:
  - Current close must be above the upper Bollinger Band (20,2).
  - Must be accompanied by a strong green candle (close > open by >= 2%).
  - Confidence capped at 70% — breakouts above BB Upper often mean-revert.
  - Implication: Bullish (momentum) — always mention overbought risk in description.
GENERAL:
  - Do not assign confidence > 80% to any unconfirmed (pre-breakout) pattern.
  - A pattern that is "possibly forming" should be described honestly in the description field.

Return this exact structure:
{{
  "patterns": [
    {{
      "name": "Pattern name e.g. Cup & Handle",
      "confidence": 0.75,
      "implication": "Bullish",
      "description": "2-3 sentence plain English explanation of what this pattern means for the stock right now.",
      "target_price": 0.0,
      "stop_loss": 0.0,
      "trend_lines": [
        {{
          "start_date": "YYYY-MM-DD",
          "end_date": "YYYY-MM-DD",
          "start_price": 0.0,
          "end_price": 0.0,
          "color": "green",
          "label": "Neckline"
        }}
      ]
    }}
  ],
  "summary": "One sentence overall pattern read e.g. 'Bullish continuation setup with Cup & Handle forming near 52w high.'"
}}
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": f"Detect all active chart patterns for {symbol}. Be strict — only high confidence patterns."}
    ]

    t0 = time.perf_counter()
    status = "SUCCESS"
    error_message = None
    parsed = {}
    prompt_tokens = completion_tokens = 0

    try:
        if provider == "openai":
            parsed, prompt_tokens, completion_tokens = await _call_openai(messages, model, key_map[provider])
        elif provider == "anthropic":
            parsed, prompt_tokens, completion_tokens = await _call_anthropic(messages, model, key_map[provider])
        elif provider == "xai":
            parsed, prompt_tokens, completion_tokens = await _call_xai(messages, model, key_map[provider])
    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        logger.error(f"Pattern recognition failed ({provider}): {e}")
        raise e
    finally:
        duration_ms = int((time.perf_counter() - t0) * 1000)
        await _write_call_log(
            symbol=symbol,
            provider=provider,
            model=model,
            trigger_reason="PATTERN_RECOGNITION",
            skill_id=None,
            messages=messages,
            response_dict=parsed,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            duration_ms=duration_ms,
            status=status,
            error_message=error_message,
            user_id=user_id,
        )

    return parsed


async def generate_move_explanation(
    symbol: str,
    period: str,
    gain_pct: float,
    context_data: dict,
    user_id: int = None
) -> dict:
    """
    Generates a plain-English explanation for why a stock moved X% in a given period.
    Returns: { headline, explanation, catalysts, sentiment }
    Called on-demand from the performance cards — result cached in MoveExplanation table.
    """
    ai_cfg = await _get_ai_settings()
    provider = ai_cfg["provider"]
    key_map = {
        "openai":    ai_cfg["openai_key"],
        "anthropic": ai_cfg["anthropic_key"],
        "xai":       ai_cfg["xai_key"],
    }
    if not key_map.get(provider):
        for p, k in key_map.items():
            if k:
                provider = p
                break
    if not key_map.get(provider):
        raise ValueError("No API key configured.")

    model = {
        "openai":    ai_cfg["openai_model"],
        "anthropic": ai_cfg["anthropic_model"],
        "xai":       ai_cfg["xai_model"],
    }.get(provider, "gpt-4o")

    # Fetch live news for context
    recent_news = await _fetch_symbol_news(symbol)
    news_block  = "\n".join(f"• {h}" for h in (recent_news or [])[:6]) or "No recent news found."

    import json
    summary  = context_data.get("price_summary", {})
    signals  = context_data.get("indicators", {})
    comp     = context_data.get("composite_score", "N/A")
    st_sig   = context_data.get("current_st_signal", "HOLD")
    lt_sig   = context_data.get("current_lt_signal", "HOLD")

    period_label = {
        "week":  "this week",
        "month": "this month",
        "year":  "last 52 weeks",
        "ytd":   "year to date",
    }.get(period, period)

    direction = "up" if gain_pct >= 0 else "down"

    system_prompt = f"""You are MarketMind's market analyst for NSE/BSE Indian equities.
Your job is to explain in plain English why a stock has moved significantly.
Be specific, cite real data, and avoid generic statements.

═══ STOCK ═══
Symbol        : {symbol}
Move          : {'+' if gain_pct >= 0 else ''}{gain_pct:.2f}% {direction} {period_label}
Composite Score: {comp}/100
ST Signal     : {st_sig} | LT Signal: {lt_sig}

═══ PRICE CONTEXT ═══
Period change : {summary.get('period_change_pct', 'N/A')}%
90d High/Low  : ₹{summary.get('90d_high')} / ₹{summary.get('90d_low')}
SMA 20/50/90  : ₹{summary.get('sma_20')} / ₹{summary.get('sma_50')} / ₹{summary.get('sma_90')}
Current Price : ₹{context_data.get('today', {}).get('close', 'N/A')}

═══ LIVE NEWS ═══
{news_block}

═══ RULES ═══
1. Reply ONLY as valid JSON — no text outside the object.
2. headline: one punchy line (max 12 words) explaining the move.
3. explanation: 2-3 sentences — cite specific catalysts (earnings, FII flows, sector rotation, news).
   If the move appears purely technical with no news, say so explicitly.
4. catalysts: array of 2-4 short strings, each a distinct reason.
5. sentiment: "Bullish", "Bearish", or "Neutral" based on the overall picture.
6. should_act: one of "Consider Entry", "Consider Exit", "Hold", "Watch" — your honest recommendation.
7. Never fabricate specific numbers not present in the data.

Return this exact structure:
{{
  "headline": "Short punchy explanation of the move",
  "explanation": "2-3 sentence detailed explanation citing specific catalysts.",
  "catalysts": ["Catalyst 1", "Catalyst 2", "Catalyst 3"],
  "sentiment": "Bullish",
  "should_act": "Consider Entry"
}}
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": f"Why has {symbol} moved {'+' if gain_pct >= 0 else ''}{gain_pct:.2f}% {period_label}? Give me the real reasons."}
    ]

    t0 = time.perf_counter()
    status = "SUCCESS"
    error_message = None
    parsed = {}
    prompt_tokens = completion_tokens = 0

    try:
        if provider == "openai":
            parsed, prompt_tokens, completion_tokens = await _call_openai(messages, model, key_map[provider])
        elif provider == "anthropic":
            parsed, prompt_tokens, completion_tokens = await _call_anthropic(messages, model, key_map[provider])
        elif provider == "xai":
            parsed, prompt_tokens, completion_tokens = await _call_xai(messages, model, key_map[provider])
    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        logger.error(f"Move explanation failed ({provider}): {e}")
        raise e
    finally:
        duration_ms = int((time.perf_counter() - t0) * 1000)
        await _write_call_log(
            symbol=symbol,
            provider=provider,
            model=model,
            trigger_reason="MOVE_EXPLANATION",
            skill_id=None,
            messages=messages,
            response_dict=parsed,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            duration_ms=duration_ms,
            status=status,
            error_message=error_message,
            user_id=user_id,
        )

    return parsed


async def generate_alert_levels(
    symbol: str,
    user_message: str,
    context_data: dict,
    user_id: int = None
) -> dict:
    """
    Parses the user's natural language request and extracts concrete price alert levels.
    e.g. "alert me at support" → extracts support price from chart context.
    Returns: { alerts: [{alert_type, direction, price_level, label, rationale}], reply }
    """
    ai_cfg = await _get_ai_settings()
    provider = ai_cfg["provider"]
    key_map = {
        "openai":    ai_cfg["openai_key"],
        "anthropic": ai_cfg["anthropic_key"],
        "xai":       ai_cfg["xai_key"],
    }
    if not key_map.get(provider):
        for p, k in key_map.items():
            if k:
                provider = p
                break
    if not key_map.get(provider):
        raise ValueError("No API key configured.")

    model = {
        "openai":    ai_cfg["openai_model"],
        "anthropic": ai_cfg["anthropic_model"],
        "xai":       ai_cfg["xai_model"],
    }.get(provider, "gpt-4o")

    # Fetch live news for context
    import json
    summary = context_data.get("price_summary", {})
    today   = context_data.get("today", {})
    comp    = context_data.get("composite_score", "N/A")
    current = today.get("close", "N/A")

    system_prompt = f"""You are MarketMind's alert assistant for NSE/BSE Indian equities.
The user wants to set a price alert. Extract the exact price level(s) from their request
using the chart context provided. Be precise — use actual price levels from the data.

═══ STOCK CONTEXT ═══
Symbol        : {symbol}
Current Price : ₹{current}
Composite     : {comp}/100
90d High/Low  : ₹{summary.get('90d_high')} / ₹{summary.get('90d_low')}
SMA 20/50/90  : ₹{summary.get('sma_20')} / ₹{summary.get('sma_50')} / ₹{summary.get('sma_90')}
BB Upper/Lower: ₹{summary.get('bb_upper', 'N/A')} / ₹{summary.get('bb_lower', 'N/A')}

Recent 5 Bars:
{json.dumps(summary.get('recent_5_bars', []), indent=2)}

═══ RULES ═══
1. Reply ONLY as valid JSON — no text outside the object.
2. Extract 1-3 alert levels from the user message and chart context.
3. alert_type: one of SUPPORT | RESISTANCE | TARGET | STOP_LOSS | CUSTOM
4. direction: ABOVE (trigger when price goes above level) | BELOW (trigger when price goes below)
   - SUPPORT → direction: BELOW (alert if price breaks support)
   - RESISTANCE / TARGET → direction: ABOVE (alert when price reaches it)
   - STOP_LOSS → direction: BELOW
5. price_level must be a real number — never null.
6. label: short human-readable name e.g. "SMA50 Support", "52W High Breakout"
7. rationale: one sentence why this is a meaningful level.
8. reply: friendly confirmation message to show the user.

Return this exact structure:
{{
  "reply": "I've set X alert(s) for {symbol}. You'll be notified when price hits these levels.",
  "alerts": [
    {{
      "alert_type": "SUPPORT",
      "direction": "BELOW",
      "price_level": 0.0,
      "label": "SMA50 Support",
      "rationale": "Price has bounced from SMA50 three times in the last 90 days."
    }}
  ]
}}
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_message}
    ]

    t0 = time.perf_counter()
    status = "SUCCESS"
    error_message = None
    parsed = {}
    prompt_tokens = completion_tokens = 0

    try:
        if provider == "openai":
            parsed, prompt_tokens, completion_tokens = await _call_openai(messages, model, key_map[provider])
        elif provider == "anthropic":
            parsed, prompt_tokens, completion_tokens = await _call_anthropic(messages, model, key_map[provider])
        elif provider == "xai":
            parsed, prompt_tokens, completion_tokens = await _call_xai(messages, model, key_map[provider])
    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        logger.error(f"Alert generation failed ({provider}): {e}")
        raise e
    finally:
        duration_ms = int((time.perf_counter() - t0) * 1000)
        await _write_call_log(
            symbol=symbol,
            provider=provider,
            model=model,
            trigger_reason="ALERT_GENERATION",
            skill_id=None,
            messages=messages,
            response_dict=parsed,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            duration_ms=duration_ms,
            status=status,
            error_message=error_message,
            user_id=user_id,
        )

    return parsed


SKILL_PERSONAS = {
    "sebi_forensic": {
        "name": "SEBI Forensic Analyst",
        "icon": "🔍",
        "persona": """You are a SEBI-trained forensic analyst specialising in detecting accounting manipulation,
governance red flags, and regulatory risks in NSE/BSE listed companies.
Your lens: audit quality, related-party transactions, promoter pledge, cash flow vs profit divergence,
contingent liabilities, and any history of regulatory action.
Be direct, cite specific numbers from the fundamentals, and flag anything that smells wrong.
If the stock looks clean, say so explicitly with reasons."""
    },
    "warren_buffett_quality": {
        "name": "Warren Buffett",
        "icon": "🦅",
        "persona": """You are Warren Buffett analysing an Indian equity through your value investing lens.
Your framework: Is this a wonderful business at a fair price? Look for durable competitive moats,
high and consistent ROE (>15%), low debt, strong free cash flow, predictable earnings,
and honest management. Ask: would I be happy to own this for 10 years?
Speak plainly, use analogies, and give a clear buy/hold/avoid verdict with reasoning."""
    },
    "rj_india_growth": {
        "name": "Rakesh Jhunjhunwala India Cycle",
        "icon": "🐂",
        "persona": """You are analysing this stock through the lens of India's macro growth cycle —
the way Rakesh Jhunjhunwala approached Indian equities.
Your framework: Is this sector riding a multi-year structural tailwind? Is domestic consumption,
infrastructure, or financialisation of savings driving this business?
Look for businesses that grow 20%+ for a decade as India's per-capita income rises.
Be bullish where warranted, but call out cyclical traps and commodity risks clearly."""
    },
    "sequoia_moat": {
        "name": "Sequoia Capital Moat Analyst",
        "icon": "🌲",
        "persona": """You are a Sequoia Capital analyst evaluating this company's competitive moat and scalability.
Your framework: network effects, switching costs, brand pricing power, distribution advantage,
and regulatory moats. Can this business defend its margins as it scales?
Is there a path to 10x revenue without proportionally increasing costs?
Be precise about what kind of moat exists (or doesn't) and how durable it is."""
    },
    "ark_disruptive": {
        "name": "ARK Invest Disruption Analyst",
        "icon": "🚀",
        "persona": """You are an ARK Invest analyst looking for disruptive innovation potential.
Your framework: Is this company exposed to AI, genomics, fintech disruption, EV/energy transition,
or robotics? Is it a platform business with exponential growth potential?
What is the 5-year total addressable market? Where is the S-curve inflection?
Be willing to look past near-term losses if the long-term exponential case is strong.
Call out if this is a value trap masquerading as a growth story."""
    },
    "goldman_screener": {
        "name": "Goldman Sachs Institutional Screen",
        "icon": "📊",
        "persona": """You are a Goldman Sachs equity research analyst running an institutional screen.
Your framework: earnings revision momentum, price target vs consensus, EV/EBITDA vs sector,
free cash flow yield, return on invested capital, and institutional ownership trends.
Would this pass a prime brokerage screen? Is there a catalyst in the next 90 days?
Give a specific 12-month price target with bull/base/bear scenarios and key risks."""
    },
    "peter_lynch_simple": {
        "name": "Peter Lynch Main Street",
        "icon": "🏠",
        "persona": """You are Peter Lynch evaluating this stock with your 'invest in what you know' philosophy.
Your framework: Is this a business a retail investor can understand? Is it a fast grower,
stalwart, cyclical, turnaround, or asset play? What is the PEG ratio?
Would you see this company's product or service thriving in everyday life?
Avoid jargon. Explain in plain language. Give a clear buy/pass verdict."""
    },
}


async def generate_skill_chat_response(
    symbol: str,
    skill_id: str,
    user_message: str,
    chat_history: list,
    context_data: dict,
    user_id: int = None,
) -> dict:
    """
    Responds to a chart chat message through the lens of a specific investment skill/persona.
    Returns same structure as generate_chart_chat: { reply, trend_lines }
    """
    ai_cfg = await _get_ai_settings()
    provider = ai_cfg["provider"]
    key_map = {
        "openai":    ai_cfg["openai_key"],
        "anthropic": ai_cfg["anthropic_key"],
        "xai":       ai_cfg["xai_key"],
    }
    if not key_map.get(provider):
        for p, k in key_map.items():
            if k:
                provider = p
                break
    if not key_map.get(provider):
        raise ValueError("No API key configured.")

    model = {
        "openai":    ai_cfg["openai_model"],
        "anthropic": ai_cfg["anthropic_model"],
        "xai":       ai_cfg["xai_model"],
    }.get(provider, "gpt-4o")

    skill = SKILL_PERSONAS.get(skill_id)
    if not skill:
        raise ValueError(f"Unknown skill_id: {skill_id}")

    import json
    today    = context_data.get("today", {})
    summary  = context_data.get("price_summary", {})
    signals  = context_data.get("indicators", {})
    bt       = context_data.get("backtest", {})
    news     = context_data.get("recent_news", [])
    comp     = context_data.get("composite_score", "N/A")
    st_sig   = context_data.get("current_st_signal", "HOLD")
    lt_sig   = context_data.get("current_lt_signal", "HOLD")

    bt_str = (
        f"{bt.get('cagr', 'N/A')}% CAGR, {bt.get('win_rate', 'N/A')}% Win Rate, "
        f"{bt.get('sharpe', 'N/A')} Sharpe, {bt.get('max_drawdown', 'N/A')}% Max DD"
        if bt.get("cagr") else "Not available"
    )

    news_block   = "\n".join(f"• {h}" for h in (news or [])[:5]) or "No recent news."
    weekly_block = json.dumps(summary.get("weekly_candles", [])[-6:], indent=2)
    bars_block   = json.dumps(summary.get("recent_5_bars", []), indent=2)
    fa_block     = json.dumps(signals.get("fa", {}), indent=2)
    ta_block     = json.dumps(signals.get("ta", {}), indent=2)
    mom_block    = json.dumps(signals.get("momentum", {}), indent=2)

    system_prompt = f"""{skill['persona']}

You are currently analysing {symbol} for a MarketMind user.
Stay fully in character as {skill['name']} throughout the conversation.
Use the data below to ground your analysis — never fabricate numbers.

═══ STOCK DATA ═══
Symbol          : {symbol}
Current Price   : ₹{today.get('close', 'N/A')}
Composite Score : {comp}/100
ST Signal       : {st_sig} | LT Signal: {lt_sig}
Backtest        : {bt_str}

═══ PRICE SUMMARY (90 days) ═══
Period Change   : {summary.get('period_change_pct', 'N/A')}%
90d High/Low    : ₹{summary.get('90d_high')} / ₹{summary.get('90d_low')}
SMA 20/50/90    : ₹{summary.get('sma_20')} / ₹{summary.get('sma_50')} / ₹{summary.get('sma_90')}

Weekly Candles (last 6 weeks):
{weekly_block}

Recent 5 Bars:
{bars_block}

═══ FUNDAMENTALS ═══
{fa_block}

═══ TECHNICAL INDICATORS ═══
{ta_block}

═══ MOMENTUM ═══
{mom_block}

═══ LIVE NEWS ═══
{news_block}

═══ RESPONSE RULES ═══
1. Reply ONLY as valid JSON — no text outside the object.
2. Stay in character as {skill['name']} — use their vocabulary and decision framework.
3. Be direct and opinionated. Avoid hedging everything.
4. Only include trend_lines if they directly support your analysis point.
5. Format reply in readable markdown — use **bold** for key numbers and verdicts.
6. End every reply with a clear verdict line: e.g. "**Verdict: Buy / Avoid / Watch**"

Return this exact structure:
{{
  "reply": "Your markdown-formatted analysis in character as {skill['name']}.",
  "trend_lines": [
    {{
      "start_date": "YYYY-MM-DD",
      "end_date":   "YYYY-MM-DD",
      "start_price": 0.0,
      "end_price":   0.0,
      "color":  "green",
      "label":  "Key Level"
    }}
  ]
}}
Return trend_lines as [] if none apply.
"""

    # Prepend skill context message to history so AI knows what was asked before
    skill_context_msg = {
        "role": "system",
        "content": f"[Skill activated: {skill['name']}. User is now asking through this lens.]"
    }
    messages = [{"role": "system", "content": system_prompt}] + chat_history

    # Fetch live news and inject
    recent_news = await _fetch_symbol_news(symbol)
    context_data["recent_news"] = recent_news or ["No recent news found."]

    t0 = time.perf_counter()
    status = "SUCCESS"
    error_message = None
    parsed = {}
    prompt_tokens = completion_tokens = 0

    try:
        if provider == "openai":
            parsed, prompt_tokens, completion_tokens = await _call_openai(messages, model, key_map[provider])
        elif provider == "anthropic":
            parsed, prompt_tokens, completion_tokens = await _call_anthropic(messages, model, key_map[provider])
        elif provider == "xai":
            parsed, prompt_tokens, completion_tokens = await _call_xai(messages, model, key_map[provider])
    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        logger.error(f"Skill chat failed ({provider}, {skill_id}): {e}")
        raise e
    finally:
        duration_ms = int((time.perf_counter() - t0) * 1000)
        await _write_call_log(
            symbol=symbol,
            provider=provider,
            model=model,
            trigger_reason="SKILL_CHAT",
            skill_id=skill_id,
            messages=messages,
            response_dict=parsed,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            duration_ms=duration_ms,
            status=status,
            error_message=error_message,
            user_id=user_id,
        )

    return parsed


async def generate_yearly_risk_explainer(
    year: int,
    portfolio_return: float,
    nifty_return: float,
    alpha: float,
    holdings_context: list,
    macro_context: dict,
    user_id: int = None,
) -> dict:
    """
    Generates a plain-English explanation of what drove portfolio performance
    in a specific calendar year — winners, losers, macro context, and lessons.
    Returns: { headline, what_worked, what_didnt, macro_drivers, lesson, sentiment }
    """
    ai_cfg = await _get_ai_settings()
    provider = ai_cfg["provider"]
    key_map = {
        "openai":    ai_cfg["openai_key"],
        "anthropic": ai_cfg["anthropic_key"],
        "xai":       ai_cfg["xai_key"],
    }
    if not key_map.get(provider):
        for p, k in key_map.items():
            if k:
                provider = p
                break
    if not key_map.get(provider):
        raise ValueError("No API key configured.")

    model = {
        "openai":    ai_cfg["openai_model"],
        "anthropic": ai_cfg["anthropic_model"],
        "xai":       ai_cfg["xai_model"],
    }.get(provider, "gpt-4o")

    import json

    direction       = "gained" if portfolio_return >= 0 else "lost"
    vs_nifty        = "outperformed" if alpha >= 0 else "underperformed"
    alpha_abs       = abs(alpha)
    holdings_block  = json.dumps(holdings_context, indent=2) if holdings_context else "Not available"
    macro_block     = json.dumps(macro_context, indent=2)    if macro_context    else "Not available"

    # Build known macro events per year for grounding
    known_macro = {
        2020: "COVID-19 crash (Mar), V-shaped recovery (Apr-Dec), RBI emergency rate cuts, FII selling followed by DII buying",
        2021: "Vaccine rally, Nifty +24%, mid/small-cap euphoria, crypto boom, global liquidity surge",
        2022: "Russia-Ukraine war, FII outflows of ₹1.2L cr, RBI rate hikes 250bps, Nifty flat, IT sector selloff",
        2023: "Nifty +20%, domestic flows strong, Adani crisis (Jan-Feb), PSU re-rating, capex theme",
        2024: "Election rally, FII volatility, rate cut expectations, China stimulus impact on FII allocation",
        2025: "Global uncertainty, US tariff concerns, Nifty correction from highs, IT headwinds",
        2026: "Current year — partial data only",
    }.get(year, "No specific macro context available for this year.")

    system_prompt = f"""You are MarketMind's senior portfolio analyst explaining annual performance to a retail investor.
Your job is to tell the story of what happened to their portfolio in {year} — clearly, honestly, and specifically.
Use plain English. Avoid jargon. Be direct about what went right and wrong.

═══ PERFORMANCE DATA ═══
Year              : {year}
Portfolio Return  : {'+' if portfolio_return >= 0 else ''}{portfolio_return:.2f}%
Nifty 50 Return   : {'+' if nifty_return >= 0 else ''}{nifty_return:.2f}%
Alpha             : {'+' if alpha >= 0 else ''}{alpha:.2f}% ({vs_nifty} Nifty by {alpha_abs:.2f}%)

═══ PORTFOLIO HOLDINGS CONTEXT ═══
{holdings_block}

═══ MACRO EVENTS ({year}) ═══
Known events: {known_macro}
Additional context: {macro_block}

═══ RULES ═══
1. Reply ONLY as valid JSON — no text outside the object.
2. headline: one punchy sentence summarising the year (max 15 words).
3. what_worked: 2-3 sentences on what drove gains — cite specific sectors or holdings if available.
4. what_didnt: 2-3 sentences on what dragged returns — be honest, cite specific reasons.
   If portfolio outperformed strongly, this section explains what could have been even better.
5. macro_drivers: 2-3 sentences on the macro backdrop and how it affected the portfolio.
   Ground this in the known macro events above — do not fabricate events.
6. lesson: one actionable takeaway from this year's performance.
7. sentiment: "Strong" | "Good" | "Mixed" | "Tough" | "Difficult"
   based on absolute return AND alpha combined.
8. risk_flags: array of 0-3 short strings flagging concentration/timing risks visible in the data.
   Empty array if none.

Return this exact structure:
{{
  "headline": "One punchy sentence about {year}",
  "what_worked": "What drove the gains...",
  "what_didnt": "What dragged returns or could have been better...",
  "macro_drivers": "The macro backdrop and its effect...",
  "lesson": "One actionable takeaway.",
  "sentiment": "Strong",
  "risk_flags": ["Risk 1", "Risk 2"]
}}
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": f"Explain my portfolio's {year} performance: {'+' if portfolio_return >= 0 else ''}{portfolio_return:.2f}% vs Nifty {'+' if nifty_return >= 0 else ''}{nifty_return:.2f}%. What happened and what should I learn?"}
    ]

    t0 = time.perf_counter()
    status = "SUCCESS"
    error_message = None
    parsed = {}
    prompt_tokens = completion_tokens = 0

    try:
        if provider == "openai":
            parsed, prompt_tokens, completion_tokens = await _call_openai(messages, model, key_map[provider])
        elif provider == "anthropic":
            parsed, prompt_tokens, completion_tokens = await _call_anthropic(messages, model, key_map[provider])
        elif provider == "xai":
            parsed, prompt_tokens, completion_tokens = await _call_xai(messages, model, key_map[provider])
    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        logger.error(f"Yearly explainer failed ({provider}): {e}")
        raise e
    finally:
        duration_ms = int((time.perf_counter() - t0) * 1000)
        await _write_call_log(
            symbol="PORTFOLIO",
            provider=provider,
            model=model,
            trigger_reason="YEARLY_EXPLAINER",
            skill_id=None,
            messages=messages,
            response_dict=parsed,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            duration_ms=duration_ms,
            status=status,
            error_message=error_message,
            user_id=user_id,
        )

    return parsed

# ── Portfolio AI Allocation ─────────────────────────────────────────────────────

async def generate_portfolio_allocation(amount: float, portfolio_data: list) -> dict:
    """
    AI task to dynamically distribute an amount of capital across the user's active portfolio.
    Returns a dict with 'allocations' and a 'rationale'.
    """
    ai_cfg = await _get_ai_settings()
    provider = ai_cfg["provider"]
    key_map = {
        "openai": ai_cfg["openai_key"],
        "anthropic": ai_cfg["anthropic_key"],
        "xai": ai_cfg["xai_key"],
    }
    
    if not key_map.get(provider):
        for p, k in key_map.items():
            if k:
                provider = p
                break
    
    if not key_map.get(provider):
        raise ValueError(f"No API key configured for AI Portfolio Allocation.")

    model = {
        "openai": ai_cfg["openai_model"],
        "anthropic": ai_cfg["anthropic_model"],
        "xai": ai_cfg["xai_model"],
    }.get(provider, "gpt-4o")

    portfolio_summary = json.dumps([{
        "symbol": item["symbol"],
        "composite_score": float(item["signal"]["composite_score"]) if item["signal"] and item["signal"].get("composite_score") else 50.0,
        "current_price": float(item["signal"]["current_price"]) if item["signal"] and item["signal"].get("current_price") else 1.0,
        "sector": item["sector"],
        "st_signal": item["signal"]["st_signal"] if item["signal"] else "HOLD",
    } for item in portfolio_data], indent=2)

    prompt = build_portfolio_allocation_prompt(amount, portfolio_summary)
    messages = [
        {"role": "system", "content": "You are a quantitative AI agent returning strictly valid JSON."},
        {"role": "user", "content": prompt}
    ]

    t0 = time.perf_counter()
    status = "SUCCESS"
    error_message = None
    parsed = {}
    prompt_tokens = 0
    completion_tokens = 0

    try:
        api_key = key_map[provider]
        if provider == "openai":
            parsed, prompt_tokens, completion_tokens = await _call_openai(messages, model, api_key)
        elif provider == "anthropic":
            parsed, prompt_tokens, completion_tokens = await _call_anthropic(messages, model, api_key)
        elif provider == "xai":
            parsed, prompt_tokens, completion_tokens = await _call_xai(messages, model, api_key)
            
    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        logger.error(f"AI Portfolio Allocation generation failed ({provider}): {e}")
        raise e

    finally:
        duration_ms = int((time.perf_counter() - t0) * 1000)
        await _write_call_log(
            symbol="PORT_ALLOC",
            provider=provider,
            model=model,
            trigger_reason="MANUAL_ALLOCATION",
            skill_id=None,
            messages=messages,
            response_dict=parsed,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            duration_ms=duration_ms,
            status=status,
            error_message=error_message,
        )

    return parsed


# ── Utilities ─────────────────────────────────────────────────────────────────

def _get_mock_insight(symbol: str, trigger_reason: str, error_msg: str = None) -> dict:
    short_msg = f"Short-term outlook for {symbol}: mixed signals pending live AI generation."
    long_msg = f"Long-term thesis for {symbol}: fundamentals are under review. Set an AI provider key to generate live insights."
    
    if error_msg:
        short_msg = f"Temporary disruption for {symbol}: AI generation failed."
        long_msg = f"Notice: {error_msg}. Please check your API keys or try again later if it was a network timeout."

    return {
        "short_summary": short_msg,
        "long_summary": long_msg,
        "verdict": "HOLD",
        "key_risks": ["Service unavailable" if error_msg else "No AI key configured", "Data may be stale"],
        "key_opportunities": ["Check AI Logs for error details" if error_msg else "Set OPENAI_API_KEY to unlock live analysis"],
        "sentiment_score": 0.5,
    }


def _load_skill_prompt(skill_id: str, context: dict = None) -> str:
    try:
        base_path = os.path.dirname(os.path.abspath(__file__))
        skill_path = os.path.join(base_path, "skills", f"{skill_id}.md")
        if os.path.exists(skill_path):
            with open(skill_path, "r") as f:
                content = f.read()
                
            # Basic variable substitution: {{VARIABLE}} -> context["VARIABLE"]
            if context:
                for key, val in context.items():
                    placeholder = "{{" + key + "}}"
                    content = content.replace(placeholder, str(val))
            return content
            
        logger.warning(f"Skill file not found: {skill_path}")
        return ""
    except Exception as e:
        logger.error(f"Error loading skill {skill_id}: {e}")
        return ""
