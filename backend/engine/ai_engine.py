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
    return [{"role": "user", "content": prompt}]


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
5. Only include trend_lines that directly answer the question asked.
6. Format all dates as YYYY-MM-DD matching the data above.

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
  ]
}}
Return trend_lines as [] if none apply.
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
