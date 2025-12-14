from __future__ import annotations

import asyncio
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Literal, Optional

AdjustmentAction = Literal["add", "reduce", "transfer"]
AdjustmentAgentStatus = Literal["pending", "acknowledged", "failed", "expired"]


@dataclass
class AdjustmentAgentState:
    agent_id: str
    status: AdjustmentAgentStatus = "pending"
    updated_at: float = field(default_factory=time.time)
    note: Optional[str] = None

    def serialize(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "status": self.status,
            "updated_at": self.updated_at,
            "note": self.note,
        }


@dataclass
class AdjustmentRequest:
    request_id: str
    action: AdjustmentAction
    magnitude: float
    created_at: float
    created_by: Optional[str]
    target_agents: List[str]
    target_symbols: List[str] = field(default_factory=list)
    agent_states: Dict[str, AdjustmentAgentState] = field(default_factory=dict)
    expires_at: Optional[float] = None
    payload: Dict[str, Any] = field(default_factory=dict)

    def overall_status(self) -> str:
        if not self.agent_states:
            return "completed"
        pending = 0
        failed = 0
        acknowledged = 0
        for state in self.agent_states.values():
            if state.status == "pending":
                pending += 1
            elif state.status == "failed":
                failed += 1
            elif state.status == "acknowledged":
                acknowledged += 1
        if pending == len(self.agent_states):
            return "pending"
        if pending > 0:
            return "in_progress"
        if failed > 0:
            return "failed"
        if acknowledged:
            return "completed"
        return "unknown"

    def serialize(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "action": self.action,
            "magnitude": self.magnitude,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "expires_at": self.expires_at,
            "target_agents": list(self.target_agents),
            "target_symbols": list(self.target_symbols),
            "symbols": list(self.target_symbols),
            "overall_status": self.overall_status(),
            "agents": [state.serialize() for state in self.agent_states.values()],
            "payload": dict(self.payload),
        }


class GrvtAdjustmentManager:
    """Tracks GRVT adjustment broadcasts and per-agent acknowledgements."""

    def __init__(
        self,
        *,
        history_limit: int = 40,
        expiry_seconds: float = 900.0,
    ) -> None:
        self._history_limit = max(1, int(history_limit))
        self._expiry_seconds = max(30.0, float(expiry_seconds))
        self._lock = asyncio.Lock()
        self._requests: Dict[str, AdjustmentRequest] = {}
        self._ordered_ids: List[str] = []

    async def create_request(
        self,
        *,
        action: AdjustmentAction,
        magnitude: float,
        agent_ids: Iterable[str],
        symbols: Optional[Iterable[str]] = None,
        created_by: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        agent_list = self._normalize_agent_ids(agent_ids)
        if not agent_list:
            raise ValueError("No valid agent IDs specified for adjustment request")
        symbol_list = self._normalize_symbols(symbols)
        magnitude = float(magnitude)
        if not (magnitude > 0):
            raise ValueError("Adjustment magnitude must be positive")
        request = AdjustmentRequest(
            request_id=secrets.token_hex(8),
            action=action,
            magnitude=magnitude,
            created_at=time.time(),
            created_by=(created_by or "dashboard"),
            target_agents=agent_list,
            target_symbols=symbol_list,
            expires_at=time.time() + self._expiry_seconds,
            agent_states={agent: AdjustmentAgentState(agent_id=agent) for agent in agent_list},
            payload=dict(payload or {}),
        )
        async with self._lock:
            self._prune_locked(time.time())
            self._requests[request.request_id] = request
            self._ordered_ids.insert(0, request.request_id)
            if len(self._ordered_ids) > self._history_limit:
                for expired_id in self._ordered_ids[self._history_limit :]:
                    self._requests.pop(expired_id, None)
                self._ordered_ids = self._ordered_ids[: self._history_limit]
        return request.serialize()

    async def list_requests(self, *, limit: int = 20) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), self._history_limit))
        async with self._lock:
            self._prune_locked(time.time())
            ids = self._ordered_ids[:limit]
            return [self._requests[req_id].serialize() for req_id in ids if req_id in self._requests]

    async def summary(self) -> Dict[str, Any]:
        requests = await self.list_requests(limit=self._history_limit)
        active = next((req for req in requests if req.get("overall_status") in {"pending", "in_progress"}), None)
        return {
            "requests": requests,
            "active_request": active,
            "total_requests": len(requests),
        }

    async def pending_for_agent(self, agent_id: str) -> List[Dict[str, Any]]:
        normalized = self._normalize_agent_id(agent_id)
        if not normalized:
            return []
        async with self._lock:
            self._prune_locked(time.time())
            pending: List[Dict[str, Any]] = []
            for request_id in self._ordered_ids:
                request = self._requests.get(request_id)
                if request is None:
                    continue
                state = request.agent_states.get(normalized)
                if state and state.status == "pending":
                    pending.append(
                        {
                            "request_id": request.request_id,
                            "action": request.action,
                            "magnitude": request.magnitude,
                            "created_at": request.created_at,
                            "symbols": list(request.target_symbols),
                            "payload": dict(request.payload),
                        }
                    )
            return pending

    async def acknowledge(
        self,
        *,
        request_id: str,
        agent_id: str,
        status: str,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        normalized = self._normalize_agent_id(agent_id)
        if not normalized:
            raise ValueError("Agent ID is required for acknowledgement")
        request: Optional[AdjustmentRequest] = None
        async with self._lock:
            self._prune_locked(time.time())
            request = self._requests.get(request_id)
            if request is None:
                raise KeyError(f"Adjustment request {request_id} not found")
            state = request.agent_states.get(normalized)
            if state is None:
                raise KeyError(f"Agent {normalized} not targeted by request {request_id}")
            mapped_status = self._normalize_status(status)
            state.status = mapped_status
            state.note = note
            state.updated_at = time.time()
            if mapped_status == "expired":
                state.note = note or "expired"
        return request.serialize()

    def _normalize_status(self, status: str) -> AdjustmentAgentStatus:
        text = (status or "").strip().lower()
        if text in {"ack", "acked", "acknowledged", "done", "success", "succeeded"}:
            return "acknowledged"
        if text in {"fail", "failed", "error"}:
            return "failed"
        if text in {"pending", "wait"}:
            return "pending"
        if text in {"expire", "expired"}:
            return "expired"
        raise ValueError(f"Unsupported adjustment status '{status}'")

    def _normalize_agent_ids(self, agent_ids: Iterable[str]) -> List[str]:
        normalized: List[str] = []
        for agent_id in agent_ids:
            value = self._normalize_agent_id(agent_id)
            if value and value not in normalized:
                normalized.append(value)
        return normalized

    @staticmethod
    def _normalize_agent_id(agent_id: Any) -> str:
        try:
            text = str(agent_id).strip()
        except Exception:
            return ""
        return text[:120] if text else ""

    def _normalize_symbols(self, symbols: Optional[Iterable[str]]) -> List[str]:
        if not symbols:
            return []
        normalized: List[str] = []
        for symbol in symbols:
            value = self._normalize_symbol(symbol)
            if value and value not in normalized:
                normalized.append(value)
        return normalized

    @staticmethod
    def _normalize_symbol(symbol: Any) -> str:
        try:
            text = str(symbol).strip()
        except Exception:
            return ""
        text = text.upper()
        return text[:80] if text else ""

    def _prune_locked(self, now: float) -> None:
        to_remove: List[str] = []
        for request_id, request in self._requests.items():
            if request.expires_at and now >= request.expires_at:
                for state in request.agent_states.values():
                    if state.status == "pending":
                        state.status = "expired"
                        state.note = state.note or "Expired without ACK"
                        state.updated_at = now
            if now - request.created_at > (self._expiry_seconds * 4):
                to_remove.append(request_id)
        if to_remove:
            for request_id in to_remove:
                self._requests.pop(request_id, None)
                if request_id in self._ordered_ids:
                    self._ordered_ids.remove(request_id)
