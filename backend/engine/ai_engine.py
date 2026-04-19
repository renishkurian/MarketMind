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


def _build_prompt(
    symbol: str,
    trigger_reason: str,
    price_history_df: pd.DataFrame,
    signals: dict,
    fundamentals: dict,
    skill_persona: str,
) -> str:
    price_summary = ""
    if not price_history_df.empty and len(price_history_df) >= 90:
        df_90 = price_history_df.tail(90)
        start_p = float(df_90.iloc[0]["close"])
        end_p = float(df_90.iloc[-1]["close"])
        pct = (end_p - start_p) / start_p * 100
        price_summary = (
            f"Over the last 90 days, price moved from ₹{start_p:.2f} to "
            f"₹{end_p:.2f} ({pct:+.2f}%)."
        )

    persona = skill_persona or "You are a senior financial analyst specialising in the Indian equity market."

    return f"""
{persona}

Generate an analytical insight for {symbol}.

TRIGGER: {trigger_reason}

PRICE ACTION:
{price_summary}

SIGNALS:
Short-Term: {signals.get('st_signal')} (Score: {signals.get('st_score')})
Long-Term : {signals.get('lt_signal')} (Score: {signals.get('lt_score')})
Confidence: {signals.get('confidence_pct')}%
Data Quality: {signals.get('data_quality')}

TECHNICAL BREAKDOWN:
{json.dumps(signals.get('indicator_breakdown', {}), indent=2)}

FUNDAMENTALS:
{json.dumps(fundamentals, indent=2)}

Return ONLY a JSON object:
{{
    "short_summary": "2–3 sentence short-term outlook.",
    "long_summary": "3–4 sentence long-term thesis.",
    "verdict": "one of: STRONG_BUY | BUY | ACCUMULATE | HOLD | WATCH | AVOID",
    "key_risks": ["Risk 1", "Risk 2", "Risk 3"],
    "key_opportunities": ["Opportunity 1", "Opportunity 2", "Opportunity 3"],
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


async def generate_fundamentals(symbol: str) -> dict:
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

    prompt = f"""
You are a top-tier equity research analyst. 
Find the MOST RECENT available fundamental data for the company with ticker: {symbol} (NSE/BSE India).

Research these specific metrics:
1. P/E Ratio (Trailing)
2. EPS (Trailing 12 Months)
3. ROE (Return on Equity %)
4. Debt to Equity Ratio
5. Revenue Growth (YoY %)
6. Market Capitalization (in INR)

Return ONLY a valid JSON object with these keys (use null if data is absolutely unavailable):
{{
    "pe_ratio": float,
    "eps": float,
    "roe": float (decimal, e.g. 0.15 for 15%),
    "debt_equity": float,
    "revenue_growth": float (decimal, e.g. 0.10 for 10%),
    "market_cap": long
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

    # Load skill
    skill_persona = _load_skill_prompt(skill_id) if skill_id else ""

    # Build prompt
    prompt = _build_prompt(symbol, trigger_reason, price_history_df, signals, fundamentals, skill_persona)
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


def _load_skill_prompt(skill_id: str) -> str:
    try:
        base_path = os.path.dirname(os.path.abspath(__file__))
        skill_path = os.path.join(base_path, "skills", f"{skill_id}.md")
        if os.path.exists(skill_path):
            with open(skill_path, "r") as f:
                return f.read()
        logger.warning(f"Skill file not found: {skill_path}")
        return ""
    except Exception as e:
        logger.error(f"Error loading skill {skill_id}: {e}")
        return ""
