"""Single-account, bounded Aster market-stability canary.

The default mode is dry-run. Live mode requires two explicit switches, starts
only from a flat target position, runs a bounded number of cycles, opens one
small market position, closes it reduce-only, confirms the position is flat,
and records BBO impact/recovery plus execution wear.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

import aiohttp
import dotenv

from strategies.aster_cost_monitor import BBO, DEFAULT_FEE_RATE, DEFAULT_REST_URL
from strategies.aster_neutral_manager import AsterAccountClient, AsterAccountSpec, AsterNeutralSettings

LOGGER = logging.getLogger("strategies.aster_market_stability_canary")


def _decimal(value: Any, label: str) -> Decimal:
    try:
        result = Decimal(str(value).strip())
    except Exception as exc:
        raise ValueError(f"{label} must be a decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{label} must be finite")
    return result


def _json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Mapping):
        return {str(key): _json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(item) for item in value]
    return value


@dataclass(slots=True)
class CanarySettings:
    account: AsterAccountSpec
    symbol: str = "XAUUSD1"
    rest_url: str = DEFAULT_REST_URL
    direction: str = "BUY"
    quantity: Decimal = Decimal("0.002")
    max_quantity: Decimal = Decimal("0.01")
    max_spread_bps: Decimal = Decimal("8")
    max_slippage_bps: Decimal = Decimal("20")
    max_estimated_wear: Decimal = Decimal("5")
    fee_rate: Decimal = DEFAULT_FEE_RATE
    cycles: int = 1
    cycle_cooldown_seconds: float = 30.0
    position_tolerance: Decimal = Decimal("0.00000001")
    recovery_timeout_seconds: float = 10.0
    recovery_poll_seconds: float = 0.25
    recovery_spread_tolerance_bps: Decimal = Decimal("1")
    live: bool = False
    confirm_live: bool = False
    output_path: str = "logs/aster_market_stability_canary.jsonl"

    def validate(self) -> None:
        self.symbol = self.symbol.strip().upper()
        self.direction = self.direction.strip().upper()
        if self.direction not in {"BUY", "SELL"}:
            raise ValueError("direction must be BUY or SELL")
        if self.quantity <= 0 or self.max_quantity <= 0 or self.quantity > self.max_quantity:
            raise ValueError("quantity must be positive and no greater than max_quantity")
        if self.max_spread_bps <= 0 or self.max_slippage_bps <= 0 or self.max_estimated_wear <= 0:
            raise ValueError("spread, slippage, and wear caps must be positive")
        if self.cycles < 1 or self.cycles > 10:
            raise ValueError("cycles must be between 1 and 10")
        if self.cycle_cooldown_seconds < 0 or self.recovery_timeout_seconds <= 0 or self.recovery_poll_seconds <= 0:
            raise ValueError("timing settings are invalid")
        if self.live and not self.confirm_live:
            raise ValueError("live canary requires confirm_live=true")
        if not self.account.uses_pro_api:
            raise ValueError("Aster canary requires Pro API user/signer credentials")
        probe = AsterNeutralSettings(
            main=AsterAccountSpec(
                "main",
                user_address=self.account.user_address,
                signer_address=self.account.signer_address,
                signer_private_key=self.account.signer_private_key,
            ),
            sub=AsterAccountSpec("sub", api_key="unused", api_secret="unused"),
            symbol=self.symbol,
            rest_url=self.rest_url,
        )
        probe.validate()


@dataclass(frozen=True, slots=True)
class MarketRules:
    quantity_step: Decimal
    min_quantity: Decimal
    min_notional: Decimal

    def normalize(self, quantity: Decimal) -> Decimal:
        units = (quantity / self.quantity_step).to_integral_value(rounding=ROUND_DOWN)
        return units * self.quantity_step


class UnknownExecutionState(RuntimeError):
    pass


class AsterMarketStabilityCanary:
    def __init__(self, settings: CanarySettings):
        settings.validate()
        self.settings = settings
        self.session: Optional[aiohttp.ClientSession] = None
        self.client: Optional[AsterAccountClient] = None
        self.rules: Optional[MarketRules] = None

    async def start(self) -> None:
        self.session = aiohttp.ClientSession()
        neutral_settings = AsterNeutralSettings(
            main=self.settings.account,
            sub=AsterAccountSpec("sub", api_key="unused", api_secret="unused"),
            symbol=self.settings.symbol,
            rest_url=self.settings.rest_url,
        )
        self.client = AsterAccountClient(self.settings.account, neutral_settings, self.session)
        self.rules = await self._fetch_rules()
        mode = await self.client.request("GET", "/fapi/v3/positionSide/dual")
        if not isinstance(mode, Mapping) or str(mode.get("dualSidePosition", "")).strip().casefold() not in {"false", "0"}:
            raise RuntimeError("Aster stability canary requires one-way position mode")

    async def _public_json(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Any:
        assert self.session is not None
        async with self.session.get(
            f"{self.settings.rest_url.rstrip('/')}{path}",
            params=params,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as response:
            text = await response.text()
            if response.status != 200:
                raise RuntimeError(f"Aster public HTTP {response.status}: {text[:300]}")
            return json.loads(text)

    async def _fetch_rules(self) -> MarketRules:
        payload = await self._public_json("/fapi/v1/exchangeInfo")
        rows = payload.get("symbols", []) if isinstance(payload, Mapping) else []
        item = next((row for row in rows if isinstance(row, Mapping) and str(row.get("symbol", "")).upper() == self.settings.symbol), None)
        if item is None or str(item.get("status", "")).upper() != "TRADING":
            raise RuntimeError(f"Aster symbol {self.settings.symbol} is not trading")
        step = minimum = min_notional = Decimal("0")
        for filter_item in item.get("filters", []) or []:
            if not isinstance(filter_item, Mapping):
                continue
            if filter_item.get("filterType") == "LOT_SIZE":
                step = _decimal(filter_item.get("stepSize"), "stepSize")
                minimum = _decimal(filter_item.get("minQty"), "minQty")
            elif filter_item.get("filterType") == "MIN_NOTIONAL":
                min_notional = _decimal(filter_item.get("notional"), "minNotional")
        if step <= 0 or minimum <= 0:
            raise RuntimeError("Aster market quantity rules are unavailable")
        return MarketRules(step, minimum, min_notional)

    async def fetch_bbo(self) -> BBO:
        payload = await self._public_json("/fapi/v1/ticker/bookTicker", {"symbol": self.settings.symbol})
        bid = _decimal(payload.get("bidPrice"), "bidPrice")
        ask = _decimal(payload.get("askPrice"), "askPrice")
        if bid <= 0 or ask <= bid:
            raise RuntimeError("invalid Aster BBO")
        return BBO(time.time(), bid, ask)

    async def fetch_position(self) -> Decimal:
        assert self.client is not None
        payload = await self.client.request("GET", "/fapi/v3/positionRisk", {"symbol": self.settings.symbol})
        rows = payload if isinstance(payload, list) else payload.get("positions", []) if isinstance(payload, Mapping) else []
        total = Decimal("0")
        for row in rows:
            if isinstance(row, Mapping) and str(row.get("symbol", "")).upper() == self.settings.symbol:
                total += _decimal(row.get("positionAmt", row.get("position_amt", 0)), "positionAmt")
        return total

    def estimate_wear(self, bbo: BBO, quantity: Decimal) -> Decimal:
        notional = quantity * bbo.mid
        return quantity * bbo.spread + notional * self.settings.fee_rate * Decimal("2")

    async def submit_market(self, side: str, quantity: Decimal, *, reduce_only: bool) -> Dict[str, Any]:
        assert self.client is not None
        params = {
            "symbol": self.settings.symbol,
            "side": side,
            "type": "MARKET",
            "quantity": format(quantity, "f"),
            "positionSide": "BOTH",
            "reduceOnly": "true" if reduce_only else "false",
            "newOrderRespType": "RESULT",
            "newClientOrderId": f"stability-{uuid.uuid4().hex[:24]}",
        }
        try:
            result = await self.client.request("POST", "/fapi/v3/order", params)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise UnknownExecutionState(f"Aster order status is unknown: {exc}") from exc
        if not isinstance(result, Mapping):
            raise UnknownExecutionState("Aster returned a non-object order response")
        status = str(result.get("status", "")).upper()
        executed = _decimal(result.get("executedQty", result.get("cumQty", 0)), "executedQty")
        average_price = _decimal(result.get("avgPrice", 0), "avgPrice")
        if status != "FILLED" or executed <= 0 or average_price <= 0:
            raise UnknownExecutionState(f"Aster did not confirm a full fill: status={status}, executed={executed}")
        return {"status": status, "executed_quantity": executed, "average_price": average_price, "raw": _json(result)}

    async def wait_for_position(self, expected_sign: int, timeout: float = 5.0) -> Decimal:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            position = await self.fetch_position()
            if expected_sign == 0 and abs(position) <= self.settings.position_tolerance:
                return position
            if expected_sign > 0 and position > self.settings.position_tolerance:
                return position
            if expected_sign < 0 and position < -self.settings.position_tolerance:
                return position
            await asyncio.sleep(0.25)
        raise UnknownExecutionState("position confirmation timed out")

    async def emergency_flatten(self) -> Dict[str, Any]:
        """Reduce the authoritative residual position; never infer from orders."""

        attempts: List[Dict[str, Any]] = []
        for attempt in range(1, 4):
            try:
                position = await self.fetch_position()
            except Exception as exc:
                attempts.append({"attempt": attempt, "error": f"position read failed: {exc}"})
                await asyncio.sleep(0.5)
                continue
            if abs(position) <= self.settings.position_tolerance:
                return {"status": "flat", "attempts": attempts, "residual": position}
            side = "SELL" if position > 0 else "BUY"
            try:
                result = await self.submit_market(side, abs(position), reduce_only=True)
                attempts.append({"attempt": attempt, "position": position, "result": result})
            except Exception as exc:
                attempts.append({"attempt": attempt, "position": position, "error": str(exc)})
            await asyncio.sleep(0.5)
        residual = await self.fetch_position()
        if abs(residual) > self.settings.position_tolerance:
            raise RuntimeError(f"emergency flatten failed; residual={residual}; attempts={_json(attempts)}")
        return {"status": "flat", "attempts": attempts, "residual": residual}

    async def measure_recovery(self, baseline: BBO) -> Dict[str, Any]:
        started = time.monotonic()
        deadline = time.monotonic() + self.settings.recovery_timeout_seconds
        samples = 0
        last = baseline
        while time.monotonic() < deadline:
            last = await self.fetch_bbo()
            samples += 1
            if last.spread_bps <= baseline.spread_bps + self.settings.recovery_spread_tolerance_bps:
                return {"recovered": True, "seconds": time.monotonic() - started, "samples": samples, "final_bbo": _json({"bid": last.bid, "ask": last.ask, "spread_bps": last.spread_bps})}
            await asyncio.sleep(self.settings.recovery_poll_seconds)
        return {"recovered": False, "seconds": self.settings.recovery_timeout_seconds, "samples": samples, "final_bbo": _json({"bid": last.bid, "ask": last.ask, "spread_bps": last.spread_bps})}

    async def run_cycle(self, cycle: int) -> Dict[str, Any]:
        assert self.rules is not None
        initial_position = await self.fetch_position()
        if abs(initial_position) > self.settings.position_tolerance:
            raise RuntimeError(f"dedicated canary account must start flat; position={initial_position}")
        before = await self.fetch_bbo()
        quantity = self.rules.normalize(self.settings.quantity)
        if quantity < self.rules.min_quantity or quantity > self.settings.max_quantity:
            raise RuntimeError("normalized quantity is outside configured bounds")
        if quantity * before.mid < self.rules.min_notional:
            raise RuntimeError("canary order is below Aster minimum notional")
        if before.spread_bps > self.settings.max_spread_bps:
            return {"status": "skipped", "reason": "spread_above_cap", "spread_bps": before.spread_bps}
        estimated = self.estimate_wear(before, quantity)
        if estimated > self.settings.max_estimated_wear:
            return {"status": "skipped", "reason": "estimated_wear_above_cap", "estimated_wear": estimated}
        if not self.settings.live:
            return {"status": "dry_run", "cycle": cycle, "quantity": quantity, "bbo": _json({"bid": before.bid, "ask": before.ask, "spread_bps": before.spread_bps}), "estimated_wear": estimated}

        open_started = time.monotonic()
        try:
            opened = await self.submit_market(self.settings.direction, quantity, reduce_only=False)
            open_latency = time.monotonic() - open_started
            await self.wait_for_position(1 if self.settings.direction == "BUY" else -1)
            after_open = await self.fetch_bbo()
            close_side = "SELL" if self.settings.direction == "BUY" else "BUY"
            close_started = time.monotonic()
            closed = await self.submit_market(close_side, opened["executed_quantity"], reduce_only=True)
            close_latency = time.monotonic() - close_started
            residual = await self.wait_for_position(0)
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await asyncio.shield(self.emergency_flatten())
            raise
        except Exception as exc:
            recovery_result: Dict[str, Any]
            try:
                recovery_result = await self.emergency_flatten()
            except Exception as recovery_exc:
                recovery_result = {"status": "failed", "error": str(recovery_exc)}
            failure = _json({
                "status": "failed_after_open_attempt",
                "cycle": cycle,
                "timestamp": time.time(),
                "symbol": self.settings.symbol,
                "error": str(exc),
                "emergency_flatten": recovery_result,
            })
            self._write_record(failure)
            raise RuntimeError(f"canary cycle failed; emergency_flatten={recovery_result}") from exc
        after_close = await self.fetch_bbo()
        recovery = await self.measure_recovery(before)
        open_reference = before.ask if self.settings.direction == "BUY" else before.bid
        close_reference = after_open.bid if close_side == "SELL" else after_open.ask
        open_slippage_bps = abs(opened["average_price"] - open_reference) / open_reference * Decimal("10000")
        close_slippage_bps = abs(closed["average_price"] - close_reference) / close_reference * Decimal("10000")
        if open_slippage_bps > self.settings.max_slippage_bps or close_slippage_bps > self.settings.max_slippage_bps:
            LOGGER.error("Canary slippage exceeded cap after the position was safely closed")
        traded_notional = opened["executed_quantity"] * opened["average_price"] + closed["executed_quantity"] * closed["average_price"]
        fees = traded_notional * self.settings.fee_rate
        realized_wear = (
            (opened["average_price"] - closed["average_price"]) * opened["executed_quantity"]
            if self.settings.direction == "BUY"
            else (closed["average_price"] - opened["average_price"]) * opened["executed_quantity"]
        ) + fees
        record = _json({
            "status": "completed",
            "cycle": cycle,
            "timestamp": time.time(),
            "symbol": self.settings.symbol,
            "direction": self.settings.direction,
            "quantity": opened["executed_quantity"],
            "before": {"bid": before.bid, "ask": before.ask, "spread_bps": before.spread_bps},
            "after_open": {"bid": after_open.bid, "ask": after_open.ask, "spread_bps": after_open.spread_bps},
            "after_close": {"bid": after_close.bid, "ask": after_close.ask, "spread_bps": after_close.spread_bps},
            "open": opened,
            "close": closed,
            "open_latency_seconds": open_latency,
            "close_latency_seconds": close_latency,
            "open_slippage_bps": open_slippage_bps,
            "close_slippage_bps": close_slippage_bps,
            "estimated_wear": estimated,
            "realized_wear": realized_wear,
            "estimated_fees": fees,
            "residual_position": residual,
            "recovery": recovery,
        })
        self._write_record(record)
        return record

    def _write_record(self, record: Mapping[str, Any]) -> None:
        path = Path(self.settings.output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_json(record), ensure_ascii=False, separators=(",", ":")) + "\n")

    async def run(self) -> None:
        await self.start()
        try:
            for cycle in range(1, self.settings.cycles + 1):
                result = await self.run_cycle(cycle)
                LOGGER.info("Aster stability canary result: %s", result)
                if result.get("status") not in {"completed", "dry_run", "skipped"}:
                    break
                if cycle < self.settings.cycles and self.settings.cycle_cooldown_seconds:
                    await asyncio.sleep(self.settings.cycle_cooldown_seconds)
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None


def _env_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().casefold() in {"1", "true", "yes", "on"}


def settings_from_env(env_file: Optional[str]) -> CanarySettings:
    values: Dict[str, Any] = dict(os.environ)
    if env_file:
        values.update({key: value for key, value in dotenv.dotenv_values(env_file).items() if value is not None})
    account = AsterAccountSpec(
        "canary",
        user_address=str(values.get("ASTER_CANARY_USER_ADDRESS", "") or ""),
        signer_address=str(values.get("ASTER_CANARY_SIGNER_ADDRESS", "") or ""),
        signer_private_key=str(values.get("ASTER_CANARY_SIGNER_PRIVATE_KEY", "") or ""),
    )
    return CanarySettings(
        account=account,
        symbol=str(values.get("ASTER_CANARY_SYMBOL", "XAUUSD1")),
        rest_url=str(values.get("ASTER_CANARY_REST_URL", DEFAULT_REST_URL)).rstrip("/"),
        direction=str(values.get("ASTER_CANARY_DIRECTION", "BUY")),
        quantity=_decimal(values.get("ASTER_CANARY_QUANTITY", "0.002"), "quantity"),
        max_quantity=_decimal(values.get("ASTER_CANARY_MAX_QUANTITY", "0.01"), "max quantity"),
        max_spread_bps=_decimal(values.get("ASTER_CANARY_MAX_SPREAD_BPS", "8"), "max spread"),
        max_slippage_bps=_decimal(values.get("ASTER_CANARY_MAX_SLIPPAGE_BPS", "20"), "max slippage"),
        max_estimated_wear=_decimal(values.get("ASTER_CANARY_MAX_ESTIMATED_WEAR", "5"), "max wear"),
        cycles=int(values.get("ASTER_CANARY_CYCLES", "1")),
        cycle_cooldown_seconds=float(values.get("ASTER_CANARY_CYCLE_COOLDOWN_SECONDS", "30")),
        live=_env_bool(values.get("ASTER_CANARY_LIVE"), False),
        confirm_live=_env_bool(values.get("ASTER_CANARY_CONFIRM_LIVE"), False),
        output_path=str(values.get("ASTER_CANARY_OUTPUT_PATH", "logs/aster_market_stability_canary.jsonl")),
    )


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Bounded Aster live market-stability canary")
    parser.add_argument("--env-file")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--confirm-live", action="store_true")
    parser.add_argument("--cycles", type=int)
    args = parser.parse_args(list(argv) if argv is not None else None)
    settings = settings_from_env(args.env_file)
    if args.live:
        settings.live = True
    if args.confirm_live:
        settings.confirm_live = True
    if args.cycles is not None:
        settings.cycles = args.cycles
    settings.validate()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(AsterMarketStabilityCanary(settings).run())
    except KeyboardInterrupt:
        LOGGER.info("Aster stability canary stopped")


if __name__ == "__main__":
    main()
