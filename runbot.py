#!/usr/bin/env python3
"""
Modular Trading Bot - Supports multiple exchanges
"""

import argparse
import asyncio
import logging
import os
from pathlib import Path
import sys
from decimal import Decimal
from trading_bot import TradingBot, TradingConfig
from exchanges import ExchangeFactory

try:
    import dotenv  # type: ignore
except ImportError:  # pragma: no cover - runtime dependency guard
    dotenv = None  # type: ignore


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Modular Trading Bot - Supports multiple exchanges')

    # Exchange selection
    parser.add_argument('--exchange', type=str, default='edgex',
                        choices=ExchangeFactory.get_supported_exchanges(),
                        help='Exchange to use (default: edgex). '
                             f'Available: {", ".join(ExchangeFactory.get_supported_exchanges())}')

    # Trading parameters
    parser.add_argument('--ticker', type=str, default='ETH',
                        help='Ticker (default: ETH)')
    parser.add_argument('--quantity', type=Decimal, default=Decimal(0.1),
                        help='Order quantity (default: 0.1)')
    parser.add_argument('--take-profit', type=Decimal, default=Decimal(0.02),
                        help='Take profit in USDT (default: 0.02)')
    parser.add_argument('--direction', type=str, default='buy', choices=['buy', 'sell'],
                        help='Direction of the bot (default: buy)')
    parser.add_argument('--max-orders', type=int, default=40,
                        help='Maximum number of active orders (default: 40)')
    parser.add_argument('--wait-time', type=int, default=450,
                        help='Wait time between orders in seconds (default: 450)')
    parser.add_argument('--env-file', type=str, default=".env",
                        help=".env file path (default: .env)")
    parser.add_argument('--grid-step', type=str, default='-100',
                        help='The minimum distance in percentage to the next close order price (default: -100)')
    parser.add_argument('--stop-price', type=Decimal, default=-1,
                        help='Price to stop trading and exit. Buy: exits if price >= stop-price.'
                        'Sell: exits if price <= stop-price. (default: -1, no stop)')
    parser.add_argument('--pause-price', type=Decimal, default=-1,
                        help='Pause trading and wait. Buy: pause if price >= pause-price.'
                        'Sell: pause if price <= pause-price. (default: -1, no pause)')
    parser.add_argument('--boost', action='store_true',
                        help='Use the Boost mode for volume boosting')

    parser.add_argument('--coordinator-url', type=str, default=None,
                        help='Optional coordinator base URL (or COORDINATOR_URL)')
    parser.add_argument('--coordinator-vps-id', type=str, default=None,
                        help='Unique coordinator node ID (or COORDINATOR_VPS_ID)')
    parser.add_argument('--coordinator-alias', type=str, default=None,
                        help='Dashboard display name (or COORDINATOR_ALIAS)')
    parser.add_argument('--coordinator-user', type=str, default=None,
                        help='Coordinator basic auth username (or COORDINATOR_USER)')
    parser.add_argument('--coordinator-password', type=str, default=None,
                        help='Coordinator basic auth password (or COORDINATOR_PASSWORD)')
    parser.add_argument('--coordinator-poll-interval', type=float, default=None,
                        help='Seconds between command polls')
    parser.add_argument('--coordinator-metrics-interval', type=float, default=None,
                        help='Seconds between metrics pushes')

    return parser.parse_args()


def setup_logging(log_level: str):
    """Setup global logging configuration."""
    # Convert string level to logging constant
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Clear any existing handlers to prevent duplicates
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Configure root logger WITHOUT adding a console handler
    # This prevents duplicate logs when TradingLogger adds its own console handler
    root_logger.setLevel(level)

    # Suppress websockets debug logs unless DEBUG level is explicitly requested
    if log_level.upper() != 'DEBUG':
        logging.getLogger('websockets').setLevel(logging.WARNING)

    # Suppress other noisy loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

    # Suppress Lighter SDK debug logs
    logging.getLogger('lighter').setLevel(logging.WARNING)
    # Also suppress any root logger DEBUG messages that might be coming from Lighter
    if log_level.upper() != 'DEBUG':
        # Set root logger to WARNING to suppress DEBUG messages from Lighter SDK
        root_logger.setLevel(logging.WARNING)


async def main():
    """Main entry point."""
    args = parse_arguments()

    # Setup logging first
    setup_logging("WARNING")

    # Validate boost-mode can only be used with aster and backpack exchange
    if args.boost and args.exchange.lower() != 'aster' and args.exchange.lower() != 'backpack':
        print(f"Error: --boost can only be used when --exchange is 'aster' or 'backpack'. "
              f"Current exchange: {args.exchange}")
        sys.exit(1)

    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f"Env file not find: {env_path.resolve()}")
        sys.exit(1)
    if dotenv is None:
        print("Error: python-dotenv is required. Install project requirements first.")
        sys.exit(1)
    dotenv.load_dotenv(args.env_file)

    coordinator_url = args.coordinator_url or os.getenv("COORDINATOR_URL")
    coordinator_vps_id = args.coordinator_vps_id or os.getenv("COORDINATOR_VPS_ID")
    coordinator_alias = args.coordinator_alias or os.getenv("COORDINATOR_ALIAS")
    coordinator_user = args.coordinator_user or os.getenv("COORDINATOR_USER")
    coordinator_password = args.coordinator_password or os.getenv("COORDINATOR_PASSWORD")

    if bool(coordinator_url) != bool(coordinator_vps_id):
        print("Error: coordinator URL and VPS ID must be configured together")
        sys.exit(1)
    if bool(coordinator_user) != bool(coordinator_password):
        print("Error: coordinator username and password must be configured together")
        sys.exit(1)

    default_poll_interval = TradingConfig.__dataclass_fields__["coordinator_poll_interval"].default
    default_metrics_interval = TradingConfig.__dataclass_fields__["coordinator_metrics_interval"].default

    poll_interval = args.coordinator_poll_interval
    if poll_interval is None:
        try:
            poll_interval = float(os.getenv("COORDINATOR_POLL_INTERVAL", default_poll_interval))
        except (TypeError, ValueError):
            print("Warning: invalid COORDINATOR_POLL_INTERVAL; using default")
            poll_interval = default_poll_interval

    metrics_interval = args.coordinator_metrics_interval
    if metrics_interval is None:
        try:
            metrics_interval = float(os.getenv("COORDINATOR_METRICS_INTERVAL", default_metrics_interval))
        except (TypeError, ValueError):
            print("Warning: invalid COORDINATOR_METRICS_INTERVAL; using default")
            metrics_interval = default_metrics_interval

    # Create configuration
    config = TradingConfig(
        ticker=args.ticker.upper(),
        contract_id='',  # will be set in the bot's run method
        tick_size=Decimal(0),
        quantity=args.quantity,
        take_profit=args.take_profit,
        direction=args.direction.lower(),
        max_orders=args.max_orders,
        wait_time=args.wait_time,
        exchange=args.exchange.lower(),
        grid_step=Decimal(args.grid_step),
        stop_price=Decimal(args.stop_price),
        pause_price=Decimal(args.pause_price),
        boost_mode=args.boost,
        coordinator_url=coordinator_url,
        coordinator_vps_id=coordinator_vps_id,
        coordinator_alias=coordinator_alias,
        coordinator_user=coordinator_user,
        coordinator_password=coordinator_password,
        coordinator_poll_interval=poll_interval,
        coordinator_metrics_interval=metrics_interval,
    )

    # Create and run the bot
    bot = TradingBot(config)
    try:
        await bot.run()
    except Exception as e:
        print(f"Bot execution failed: {e}")
        # The bot's run method already handles graceful shutdown
        return


if __name__ == "__main__":
    asyncio.run(main())
