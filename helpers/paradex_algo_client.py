from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class ParadexAlgoClientConfig:
    """Minimal config for calling Paradex algo endpoints.

    This intentionally lives in perp-dex-tools so we don't need to modify (or fork)
    the upstream `paradex-py` SDK.

    Contract:
    - base_url: like "https://api.prod.paradex.trade/v1" (no trailing slash)
    - jwt_token: L2 auth JWT string
    """

    base_url: str
    jwt_token: str
    timeout_seconds: float = 10.0


class ParadexAlgoClient:
    def __init__(self, cfg: ParadexAlgoClientConfig) -> None:
        self._cfg = cfg
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {cfg.jwt_token}"})

    def fetch_open_algo_orders(self) -> Dict[str, Any]:
        """GET /v1/algo/orders

        Returns the decoded JSON dict.
        Raises requests.RequestException for transport issues.
        """

        url = f"{self._cfg.base_url}/algo/orders"
        resp = self._session.get(url, timeout=self._cfg.timeout_seconds)
        resp.raise_for_status()
        return resp.json()


def stable_twap_match_key(
    *,
    market: str,
    side: str,
    expected_size: str,
    started_at_ms: int,
    window_ms: int = 120_000,
) -> str:
    """Build a stable key to reduce accidental cross-request matches.

    We approximate by bucketing start time into a window. This is not perfect but
    helps disambiguate concurrent TWAPs with identical params.
    """

    bucket = 0
    if started_at_ms > 0 and window_ms > 0:
        bucket = int(started_at_ms // window_ms)
    raw = json.dumps(
        {
            "market": (market or "").upper(),
            "side": (side or "").upper(),
            "expected_size": str(expected_size or ""),
            "bucket": bucket,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()