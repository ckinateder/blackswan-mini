import os
from util import get_env_var, get_logger, download_stock_data
import util
from datetime import datetime, timedelta
import asyncio
import logging
import time

from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed
from alpaca.trading.client import TradingClient

import pandas as pd

logger = get_logger(__name__)


class MainStreet:
    """Contains finnhub and alpaca api calls"""

    def __init__(self, symbols: list) -> None:
        self.FINNHUB_KEY_ID = get_env_var("FINNHUB_KEY_ID")
        self.APCA_API_KEY_ID = get_env_var("APCA_API_KEY_ID")
        self.APCA_API_SECRET_KEY = get_env_var("APCA_API_SECRET_KEY")
        self.APCA_API_PAPER = get_env_var("APCA_API_PAPER")
        self.symbols = symbols

        # open alpaca websocket
        self.alpaca_ws = StockDataStream(
            self.APCA_API_KEY_ID,
            self.APCA_API_SECRET_KEY,
            feed=DataFeed.IEX,
        )  # <- replace to 'sip' if you have PRO subscription

        # get trading client
        self.trading_client = TradingClient(
            self.APCA_API_KEY_ID,
            self.APCA_API_SECRET_KEY,
            paper=self.APCA_API_PAPER,
        )

        # define rolling dataframe
        self.rolling_bars = download_stock_data(
            self.symbols,
            start_time=datetime.now() - timedelta(days=5),
            end_time=datetime.now(),
            fill_empty=True,
        )
        logger.info("Filled rolling bars")

        # subscribe to symbols
        for s in self.symbols:
            self.alpaca_ws.subscribe_bars(self._on_bar, s)

    async def _on_bar(self, b: any):
        """Will receive a bar from the websocket every minute

        Args:
            b (any): incoming bar
        """
        b_dict = {
            "timestamp": b.timestamp,
            "symbol": b.symbol,
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
        }
        # add to rolling bars
        latest_bar = pd.DataFrame(b_dict, index=[0]).set_index(["timestamp", "symbol"])[
            ["open", "high", "low", "close", "volume"]
        ]
        self.rolling_bars = pd.concat([self.rolling_bars, latest_bar])
        print(self.rolling_bars)

    def _get_market_clock(self) -> any:
        return self.trading_client.get_clock()

    def _close_to_market_close(self) -> bool:
        """Returns true if 5 minutes from market close

        Returns:
            bool: if 5 mins from market close
        """
        clk = self._get_market_clock()
        return (clk.next_close - clk.timestamp).total_seconds() < 300

    def start(self) -> None:
        """Starts the websocket"""
        # sleep until time to open
        clk = self._get_market_clock()
        if not clk.is_open:
            duration = clk.next_open - clk.timestamp
            logger.info(f"Sleeping {duration} until market open")
            time.sleep(duration.total_seconds())
        asyncio.run(self.alpaca_ws._run_forever())


if __name__ == "__main__":
    from dotenv import load_dotenv

    logger.info("Starting MainStreet")

    load_dotenv()
    ms = MainStreet(["AAPL", "AMZN"])
    ms.start()
