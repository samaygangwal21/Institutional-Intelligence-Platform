import asyncio
import os
import re
import time
import httpx
from loguru import logger
from dotenv import load_dotenv

load_dotenv(override=True)

from ..config import GEMINI_KEYS, GEMINI_ENDPOINT

# ── Groq Config ────────────────────────────────────────────────────────────────
# llama-3.3-70b-versatile: best open-source quality. Never downgraded.
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

# ── Per-key last-use tracking (lazy asyncio objects) ──────────────────────────
_key_last_used: dict = {}
_key_locks: dict = {}

def _get_key_lock(idx: int) -> asyncio.Lock:
    if idx not in _key_locks:
        _key_locks[idx] = asyncio.Lock()
        _key_last_used[idx] = 0.0
    return _key_locks[idx]

# ── Groq caller — patient, never downgrades model ─────────────────────────────
async def _call_groq(prompt: str, max_tokens: int, temperature: float) -> str:
    if not GROQ_API_KEY:
        return ""
    logger.info(f"🔄 Calling Groq ({GROQ_MODEL})...")
    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": min(max_tokens, 8000),
        "temperature": temperature,
    }
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            # Up to 5 retries — willing to wait up to ~90s total for quality
            for attempt in range(5):
                resp = await client.post(GROQ_ENDPOINT, json=payload, headers=headers)
                if resp.status_code == 200:
                    text = resp.json()["choices"][0]["message"]["content"].strip()
                    logger.success(f"✅ Groq success ({len(text)} chars) on attempt {attempt+1}.")
                    return text
                if resp.status_code == 429:
                    err_msg = resp.json().get("error", {}).get("message", "")
                    match = re.search(r"try again in ([\d.]+)s", err_msg)
                    wait_time = float(match.group(1)) + 2.0 if match else 20.0
                    logger.warning(f"Groq TPM limit (attempt {attempt+1}/5). Waiting {wait_time:.1f}s for quota reset...")
                    await asyncio.sleep(wait_time)
                    continue
                logger.error(f"Groq HTTP {resp.status_code}: {resp.text[:300]}")
                return ""
    except Exception as e:
        logger.error(f"Groq exception: {e}")
        return ""
    return ""

# ── Gemini caller — per-key pacing ───────────────────────────────────────────
async def _call_gemini_key(idx: int, key: str, prompt: str, max_tokens: int, temperature: float) -> str | None:
    lock = _get_key_lock(idx)
    async with lock:
        elapsed = time.monotonic() - _key_last_used.get(idx, 0)
        if elapsed < 4.0:
            await asyncio.sleep(4.0 - elapsed)

        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
        }
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.post(f"{GEMINI_ENDPOINT}?key={key}", json=payload)
                _key_last_used[idx] = time.monotonic()

                if resp.status_code == 429:
                    logger.warning(f"Gemini key[{idx}] rate limited (429).")
                    return None

                resp.raise_for_status()
                text = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                logger.success(f"✅ Gemini key[{idx}] success ({len(text)} chars).")
                return text
        except Exception as e:
            logger.warning(f"Gemini key[{idx}] exception: {e}")
            return None

# ── Main LLM Router ───────────────────────────────────────────────────────────
async def call_gemini_async(prompt: str, max_tokens: int = 4096, temperature: float = 0.2) -> str:
    """
    Quality-first LLM router.
    Priority: Groq (llama-3.3-70b-versatile) → Gemini key rotation → Groq retry with patience.
    Never degrades model quality. Will wait up to ~2 minutes for rate limits to clear.
    """
    # ── Pass 1: Try Groq (fastest path when quota is fresh) ──
    if GROQ_API_KEY:
        result = await _call_groq(prompt, max_tokens, temperature)
        if result:
            return result
        logger.warning("Groq exhausted for now. Switching to Gemini key rotation...")

    # ── Pass 2: Rotate through all 5 Gemini keys ──
    active_keys = [(i, k) for i, k in enumerate(GEMINI_KEYS) if k]
    if active_keys:
        for idx, key in active_keys:
            result = await _call_gemini_key(idx, key, prompt, max_tokens, temperature)
            if result is not None:
                return result
            await asyncio.sleep(0.5)

        # ── Pass 3: All Gemini keys rate-limited — wait 60s and try one more round ──
        logger.warning("All 5 Gemini keys rate-limited. Waiting 60s for quota reset (quality preserved)...")
        await asyncio.sleep(60)
        for idx, key in active_keys:
            result = await _call_gemini_key(idx, key, prompt, max_tokens, temperature)
            if result is not None:
                return result

    # ── Pass 4: Final Groq attempt after Gemini exhaustion ──
    if GROQ_API_KEY:
        logger.warning("Final Groq attempt (Gemini fully exhausted)...")
        result = await _call_groq(prompt, max_tokens, temperature)
        if result:
            return result

    return "[LLM Error: All providers exhausted after maximum patience. Please wait 2 minutes and retry.]"

# ── Embedding Engine ─────────────────────────────────────────────────────────
async def get_embedding(text: str) -> list[float]:
    """
    Generates a 768-dimensional embedding for text.
    Priority: Google Gemini embedding-004 → sha256 mock (fallback).
    """
    # Try actual embedding-004 first
    if GEMINI_KEYS:
        key = GEMINI_KEYS[0]
        url = f"https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:embedContent?key={key}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.post(url, json={"model": "models/text-embedding-004", "content": {"parts": [{"text": text}]}})
                if r.status_code == 200:
                    return r.json().get("embedding", {}).get("values", [])
        except Exception as e:
            logger.warning(f"Embedding API failed: {e}")

    # Fallback: Hash-based mock embedding for stability during API outages
    import hashlib
    h = hashlib.sha256(text.encode()).digest()
    h_expanded = h * 24
    chunk = h_expanded[:768]
    return [float(b - 128) / 128.0 for b in chunk]
