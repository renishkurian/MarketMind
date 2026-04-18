import anthropic
import openai
import json
import logging
import pandas as pd
import os
from datetime import datetime

from backend.config import settings

logger = logging.getLogger(__name__)

async def generate_insight(symbol: str, price_history_df: pd.DataFrame, signals: dict, fundamentals: dict, trigger_reason: str, skill_id: str = None) -> dict:
    """Entry point for AI insight generation. Detects provider and routes to appropriate client."""
    provider = settings.AI_PROVIDER.lower()
    
    # 0. Load Skill Persona if requested
    skill_persona = ""
    if skill_id:
        skill_persona = _load_skill_prompt(skill_id)
    
    # 1. Check for API keys
    if provider == "anthropic" and not settings.ANTHROPIC_API_KEY:
        logger.warning("Anthropic API key not set")
        return _get_mock_insight(symbol, trigger_reason)
    elif provider == "openai" and not settings.OPENAI_API_KEY:
        logger.warning("OpenAI API key not set")
        return _get_mock_insight(symbol, trigger_reason)
    elif provider == "xai" and not settings.XAI_API_KEY:
        logger.warning("xAI API key not set")
        return _get_mock_insight(symbol, trigger_reason)

    # 2. Build common prompt
    price_summary = ""
    if not price_history_df.empty and len(price_history_df) >= 90:
        df_90 = price_history_df.tail(90)
        start_p = df_90.iloc[0]['close']
        end_p = df_90.iloc[-1]['close']
        pct_change = ((end_p - start_p) / start_p) * 100
        price_summary = f"Over the last 90 days, price moved from {start_p:.2f} to {end_p:.2f} ({pct_change:.2f}%)."
    
    prompt = f"""
    {skill_persona if skill_persona else "You are a financial analyst specializing in the Indian Stock Market."}
    
    Based on the provided data, generate an analytical insight for the stock: {symbol}.
    
    TRIGGER REASON: {trigger_reason}
    
    PRICE ACTION SUMMARY:
    {price_summary}
    
    SIGNALS:
    Short-Term Signal: {signals.get('st_signal')} (Score: {signals.get('st_score')})
    Long-Term Signal: {signals.get('lt_signal')} (Score: {signals.get('lt_score')})
    Confidence: {signals.get('confidence_pct')}%
    Data Quality: {signals.get('data_quality')}
    
    TECHNICAL INDICATOR BREAKDOWN:
    {json.dumps(signals.get('indicator_breakdown', {}))}
    
    FUNDAMENTALS:
    {json.dumps(fundamentals)}
    
    Return ONLY a JSON object exactly matching this format:
    {{
        "short_summary": "2-3 sentence short-term outlook based on technicals and recent price action.",
        "long_summary": "3-4 sentence long-term investment thesis based on fundamentals and long-term indicators.",
        "key_risks": ["Risk point 1", "Risk point 2", "Risk point 3"],
        "key_opportunities": ["Opportunity 1", "Opportunity 2", "Opportunity 3"],
        "sentiment_score": a float between 0.0 and 1.0 representing overall bullishness
    }}
    """

    try:
        # 3. Route to provider
        if provider == "anthropic":
            return await _call_anthropic(prompt)
        elif provider == "openai":
            return await _call_openai(prompt)
        elif provider == "xai":
            return await _call_xai(prompt)
        else:
            return _get_mock_insight(symbol, trigger_reason)
    except Exception as e:
        logger.error(f"Error generating AI insight for {symbol} via {provider}: {e}")
        return _get_mock_insight(symbol, trigger_reason)

async def _call_anthropic(prompt: str) -> dict:
    model = settings.ANTHROPIC_MODEL or "claude-3-5-sonnet-20240620"
    client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
    response = await client.messages.create(
        model=model,
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )
    return _parse_json_response(response.content[0].text)

async def _call_openai(prompt: str) -> dict:
    model = settings.OPENAI_MODEL or "gpt-4o"
    client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    response = await client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)

async def _call_xai(prompt: str) -> dict:
    model = settings.XAI_MODEL or "grok-beta"
    # xAI is OpenAI-API compatible
    client = openai.AsyncOpenAI(
        api_key=settings.XAI_API_KEY,
        base_url="https://api.x.ai/v1"
    )
    response = await client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )
    return _parse_json_response(response.choices[0].message.content)

def _parse_json_response(text_content: str) -> dict:
    if "```json" in text_content:
        text_content = text_content.split("```json")[1].split("```")[0].strip()
    elif "```" in text_content:
        text_content = text_content.split("```")[1].strip()
    return json.loads(text_content)

def _get_mock_insight(symbol: str, trigger_reason: str) -> dict:
    return {
        "short_summary": f"Mock short-term outlook for {symbol}. The stock is showing mixed signals in the near term but currently holds stable support levels.",
        "long_summary": f"Mock long-term investment thesis for {symbol}. Fundamentals indicate a stable position in its sector, though market headwinds may present challenges over the next year.",
        "key_risks": ["General market volatility", "Sector-specific competition", "Regulatory changes"],
        "key_opportunities": ["Potential for cost realization", "Expansion into new markets", "Strong balance sheet"],
        "sentiment_score": 0.65
    }

def _load_skill_prompt(skill_id: str) -> str:
    """Loads a skill markdown file from the engine/skills directory."""
    try:
        # Base dir for skills
        base_path = os.path.dirname(os.path.abspath(__file__))
        skill_path = os.path.join(base_path, "skills", f"{skill_id}.md")
        
        if os.path.exists(skill_path):
            with open(skill_path, "r") as f:
                return f.read()
        else:
            logger.warning(f"Skill file not found: {skill_path}")
            return ""
    except Exception as e:
        logger.error(f"Error loading skill {skill_id}: {e}")
        return ""
