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
            "anthropic_model": db_cfg.get("ANTHROPIC_MODEL", settings.ANTHROPIC_MODEL) or "claude-3-5-sonnet-20240620",
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
            "anthropic_model": settings.ANTHROPIC_MODEL or "claude-3-5-sonnet-20240620",
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
    fundamentals: dict
) -> str:
    """Fallback if no skill_id is provided or skill fails."""
    return f"""
Analyze {company_name} ({symbol}).
TRIGGER: {trigger_reason}
SIGNALS: Short-Term {signals.get('st_signal')}, Long-Term {signals.get('lt_signal')}.
Return ONLY a JSON object:
{{
    "short_summary": "2–3 sentence.",
    "long_summary": "3–4 sentence.",
    "verdict": "HOLD",
    "key_risks": [],
    "key_opportunities": [],
    "sentiment_score": 0.0
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
    prompt = f"""
You are a top-tier equity research analyst. 
Find the MOST RECENT available fundamental data for the company: {company_id} (NSE/BSE India).

Research these specific metrics:
1. P/E Ratio (Trailing)
2. EPS (Trailing 12 Months)
3. ROE (Return on Equity %)
4. Debt to Equity Ratio
5. Revenue Growth (Current YoY %)
6. Market Capitalization (in INR)
7. 3-Year Revenue CAGR (%)
8. 3-Year PAT (Net Profit) CAGR (%)
9. Operating Profit Margin (%)
10. 5-Year Average P/E Ratio
11. 3-Year Average ROE (%)

Return ONLY a valid JSON object with these keys (use null if data is absolutely unavailable):
{{
    "pe_ratio": float,
    "eps": float,
    "roe": float (decimal, e.g. 0.15 for 15%),
    "debt_equity": float,
    "revenue_growth": float (decimal),
    "market_cap": long,
    "revenue_growth_3yr": float (decimal, e.g. 0.12),
    "pat_growth_3yr": float (decimal, e.g. 0.15),
    "operating_margin": float (decimal, e.g. 0.20),
    "pe_5yr_avg": float,
    "roe_3yr_avg": float (decimal)
}}
"""
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
        
        # Build dummy StockMeta + CompositeScoreResult mapped from signals dictionary
        meta = StockMeta(
            symbol=symbol,
            isin=signals.get("isin", "N/A"),
            exchange=signals.get("exchange", "NSE"),
            sector=signals.get("sector") or fundamentals.get("sector", "N/A"),
            market_cap_cr=float(fundamentals.get("market_cap", 0)) / 10000000,
            current_price=float(signals.get("current_price", 0)),
            pe_ratio=fundamentals.get("pe_ratio"),
            roe=fundamentals.get("roe"),
            debt_equity=fundamentals.get("debt_equity"),
            revenue_growth_3yr=fundamentals.get("revenue_growth_3yr"),
        )
        setattr(meta, "company_name", company_name or symbol)
        
        csr = CompositeScoreResult(
            symbol=symbol,
            isin=meta.isin,
            fundamental_score=float(signals.get("lt_score", 0) or 0),
            technical_score=float(signals.get("st_score", 0) or 0),
            momentum_score=0.0,
            sector_rank_score=0.0,
            composite_score=0.0,
            data_confidence=float(signals.get("confidence_pct", 50)) / 100.0,
            fa_breakdown=signals.get("indicator_breakdown", {})
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
    )

    return parsed  # ← was missing: caused insights to never be saved to AIInsights table


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

async def generate_chart_chat(symbol: str, user_messages: list, context_data: dict) -> dict:
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

    sys_prompt = f"""You are MarketMind's institutional chart analyst for NSE/BSE Indian equities.

STOCK: {symbol}
COMPOSITE SCORE: {context_data.get('composite_score', 'N/A')}/100
ST SIGNAL: {context_data.get('current_st_signal')} | LT SIGNAL: {context_data.get('current_lt_signal')}

AUTHORITATIVE PRICE DATA (use these exact figures — do not estimate):
{json.dumps(context_data.get('today', {}), indent=2)}

TECHNICAL CONTEXT (90-day history):
{json.dumps(context_data.get('price_summary', {}), indent=2)}

INDICATOR SIGNALS:
{json.dumps(context_data.get('indicators', {}), indent=2)}

RECENT NEWS (use to explain fundamental triggers):
{chr(10).join(f'• {h}' for h in context_data.get('recent_news', ['None found']))}

RESPONSE RULES:
1. Reply in valid JSON only — no markdown outside the reply field.
2. Cite today's verified H/L/Close. Never invent prices.
3. Identify support/resistance levels from the weekly candles.
4. If news explains the price move, cite it specifically.
5. State buy/hold/sell conviction clearly with a reason.
6. Only add trend_lines if they directly answer the question.

Return EXACTLY this structure:
{{
  "reply": "<markdown analysis — 3-5 sentences, precise, opinionated>",
  "trend_lines": [
    {{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD",
      "start_price": 0.0, "end_price": 0.0, "color": "green", "label": "Support"}}
  ]
}}"""

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

    prompt = f"""
You are a quantitative portfolio manager.
I want to allocate a lump sum of EXACTLY {amount} among my current portfolio holdings.
Here is the portfolio state with scores:
{portfolio_summary}

Instructions:
1. Weight the distribution higher toward stocks with strong fundamentals/composite_scores and solid short-term setups.
2. The sum of allocated amounts MUST equal exactly {amount}.
3. Return the result in the following JSON format:
{{
  "rationale": "High-level reason for this distribution strategy based on the sectors and scores.",
  "allocations": [
    {{
      "symbol": "TICKER",
      "allocated_amount": 5000,
      "estimated_qty": 10,
      "reason": "Brief reason for this specific allocation weight."
    }}
  ]
}}
Ensure the response is ONLY valid JSON.
"""
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
