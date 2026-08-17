"""Validated endpoint profiles for Lighter deployments."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse


@dataclass(frozen=True)
class LighterEndpointProfile:
    """REST, WebSocket, and signing parameters that must move together."""

    name: str
    rest_url: str
    ws_url: str
    chain_id: int
    supports_l1_auto_provision: bool


CORE_MAINNET = LighterEndpointProfile(
    name="core",
    rest_url="https://mainnet.zklighter.elliot.ai",
    ws_url="wss://mainnet.zklighter.elliot.ai/stream",
    chain_id=304,
    supports_l1_auto_provision=True,
)

ROBINHOOD_MAINNET = LighterEndpointProfile(
    name="robinhood",
    rest_url="https://api.rh.lighter.xyz",
    ws_url="wss://api.rh.lighter.xyz/stream",
    chain_id=466324,
    supports_l1_auto_provision=False,
)

_PROFILES = {
    "core": CORE_MAINNET,
    "mainnet": CORE_MAINNET,
    "robinhood": ROBINHOOD_MAINNET,
    "rh": ROBINHOOD_MAINNET,
    "robinhood-mainnet": ROBINHOOD_MAINNET,
}


def _normalize_url(value: str, *, websocket: bool) -> str:
    text = str(value or "").strip().rstrip("/")
    parsed = urlparse(text)
    valid_schemes = {"ws", "wss"} if websocket else {"http", "https"}
    if parsed.scheme not in valid_schemes or not parsed.hostname:
        kind = "WebSocket" if websocket else "REST"
        raise ValueError(f"Invalid Lighter {kind} URL: {value!r}")
    return text


def _profile_from_rest_url(rest_url: str) -> Optional[LighterEndpointProfile]:
    hostname = (urlparse(rest_url).hostname or "").casefold()
    for profile in (CORE_MAINNET, ROBINHOOD_MAINNET):
        expected = (urlparse(profile.rest_url).hostname or "").casefold()
        if hostname == expected:
            return profile
    return None


def resolve_lighter_endpoint_profile(
    name: Optional[str] = None,
    *,
    rest_url: Optional[str] = None,
    ws_url: Optional[str] = None,
    chain_id: Optional[int] = None,
) -> LighterEndpointProfile:
    """Resolve and validate a known Lighter deployment.

    Known deployment URLs and chain IDs are deliberately inseparable. This
    prevents a process from reading one venue while signing transactions for
    another venue.
    """

    normalized_rest = _normalize_url(rest_url, websocket=False) if rest_url else None
    normalized_ws = _normalize_url(ws_url, websocket=True) if ws_url else None

    profile: Optional[LighterEndpointProfile] = None
    if name:
        profile = _PROFILES.get(str(name).strip().casefold())
        if profile is None:
            choices = ", ".join(sorted({item.name for item in _PROFILES.values()}))
            raise ValueError(f"Unknown Lighter environment {name!r}; expected one of: {choices}")
    elif normalized_rest:
        profile = _profile_from_rest_url(normalized_rest)
        if profile is None:
            raise ValueError(
                "Unknown LIGHTER_BASE_URL. Select a known LIGHTER_ENVIRONMENT or add a validated endpoint profile."
            )
    else:
        profile = CORE_MAINNET

    resolved_rest = normalized_rest or profile.rest_url
    resolved_ws = normalized_ws or profile.ws_url
    try:
        resolved_chain = profile.chain_id if chain_id is None else int(chain_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid Lighter chain id: {chain_id!r}") from exc

    expected_rest_host = (urlparse(profile.rest_url).hostname or "").casefold()
    actual_rest_host = (urlparse(resolved_rest).hostname or "").casefold()
    expected_ws_host = (urlparse(profile.ws_url).hostname or "").casefold()
    actual_ws_host = (urlparse(resolved_ws).hostname or "").casefold()

    if actual_rest_host != expected_rest_host:
        raise ValueError(
            f"Lighter {profile.name} REST host must be {expected_rest_host}, got {actual_rest_host}"
        )
    if actual_ws_host != expected_ws_host:
        raise ValueError(
            f"Lighter {profile.name} WebSocket host must be {expected_ws_host}, got {actual_ws_host}"
        )
    if resolved_rest != profile.rest_url:
        raise ValueError(
            f"Lighter {profile.name} REST endpoint mismatch: expected {profile.rest_url}, got {resolved_rest}"
        )
    if resolved_ws != profile.ws_url:
        raise ValueError(
            f"Lighter {profile.name} WebSocket endpoint mismatch: expected {profile.ws_url}, got {resolved_ws}"
        )
    if resolved_chain != profile.chain_id:
        raise ValueError(
            f"Lighter {profile.name} chain id must be {profile.chain_id}, got {resolved_chain}"
        )

    return LighterEndpointProfile(
        name=profile.name,
        rest_url=resolved_rest,
        ws_url=resolved_ws,
        chain_id=resolved_chain,
        supports_l1_auto_provision=profile.supports_l1_auto_provision,
    )
