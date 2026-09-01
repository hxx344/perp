# Aster Paper Execution Strategy

This educational module is paper-only. It reads the public Aster BBO and
simulates a market open followed by a reduce/close order. It never loads API
credentials and the live adapter intentionally raises an exception.

Run it with:

```bash
python -m strategies.aster_volume_paper_strategy \
  --symbol XAUUSD1 \
  --max-spread-bps 8 \
  --cycle-notional 100 \
  --max-position-quantity 0.01 \
  --cycles 10
```

The simulation uses ask-to-bid crossing for a BUY cycle and bid-to-ask
crossing for a SELL cycle. It charges `0.009%` per side, records total
notional, fees, spread wear, skipped cycles, and the next direction. The
state-machine boundaries are the same places where a legitimate execution
adapter would later handle order acknowledgement, partial fills, position
reconciliation, and a kill switch; no live implementation is included here.
