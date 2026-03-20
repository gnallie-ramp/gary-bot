"""Claude API client with retry logic and JSON parsing."""

import json
import logging
import re
import time

import anthropic

from config import ANTHROPIC_API_KEY, CLAUDE_MODEL

logger = logging.getLogger(__name__)


def get_client():
    """Return an ``anthropic.Anthropic`` client configured with the API key."""
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=90)


def call_claude(prompt, max_tokens=1500, model=None, system=None):
    """Send a prompt to Claude and return the text response.

    Retries up to 3 times on ``RateLimitError`` and ``APIConnectionError``
    with exponential backoff (2^attempt seconds).

    Parameters
    ----------
    prompt : str
        The user message content.
    max_tokens : int
        Maximum tokens in the response.
    model : str, optional
        Model override; defaults to ``CLAUDE_MODEL`` from config.
    system : str, optional
        System prompt.

    Returns
    -------
    str
        The text content of the first content block.
    """
    client = get_client()
    model = model or CLAUDE_MODEL
    max_retries = 3

    kwargs = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        kwargs["system"] = system

    for attempt in range(max_retries):
        try:
            response = client.messages.create(**kwargs)
            return response.content[0].text
        except anthropic.RateLimitError:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(
                    "Claude rate limited — retrying in %ds (attempt %d/%d)",
                    wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
            else:
                raise
        except anthropic.APIConnectionError:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(
                    "Claude connection error — retrying in %ds (attempt %d/%d)",
                    wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
            else:
                raise


def call_claude_json(prompt, max_tokens=1800, model=None, system=None):
    """Call Claude and parse the response as JSON.

    Strips markdown code fences (```json ... ```) before parsing.

    Parameters
    ----------
    prompt : str
        The user message content.
    max_tokens : int
        Maximum tokens in the response.
    model : str, optional
        Model override; defaults to ``CLAUDE_MODEL``.
    system : str, optional
        System prompt.

    Returns
    -------
    dict
        Parsed JSON object.
    """
    raw = call_claude(prompt, max_tokens=max_tokens, model=model, system=system)
    # Strip markdown code fences if present
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned.strip())
    return json.loads(cleaned)
