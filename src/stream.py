import os
from util import get_env_var, get_logger, download_stock_data
import util
from alpaca_trade_api.common import URL
from alpaca_trade_api.stream import Stream
from datetime import datetime, timedelta
import asyncio
import logging
import pandas as pd

logger = get_logger(__name__)


class MainStreet:
    """Contains finnhub and alpaca api calls"""

    def __init__(self, symbols: list) -> None:
        self.FINNHUB_KEY_ID = get_env_var("FINNHUB_KEY_ID")
        self.APCA_API_KEY_ID = get_env_var("APCA_API_KEY_ID")
        self.APCA_API_SECRET_KEY = get_env_var("APCA_API_SECRET_KEY")
        self.APCA_API_BASE_URL = get_env_var("APCA_API_BASE_URL")
        self.APCA_API_DATA_URL = get_env_var("APCA_API_DATA_URL")
        self.APCA_API_PAPER = get_env_var("APCA_API_PAPER")
        self.symbols = symbols

        # open alpaca websocket
        self.alpaca_ws = Stream(
            self.APCA_API_KEY_ID,
            self.APCA_API_SECRET_KEY,
            base_url=URL(self.APCA_API_BASE_URL),
            data_feed="iex",
        )  # <- replace to 'sip' if you have PRO subscription

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

    async def _on_bar(self, b):
        # add to rolling bars
        b._raw["timestamp"] = datetime.fromtimestamp(
            b._raw["timestamp"] / 1e9, tz=util.utc
        )
        bar = pd.DataFrame(b._raw, index=[0]).set_index(["timestamp", "symbol"])[
            ["open", "high", "low", "close", "volume"]
        ]
        self.rolling_bars = pd.concat([self.rolling_bars, bar])
        print(self.rolling_bars)

    def start(self) -> None:
        """Starts the websocket"""
        asyncio.run(self.alpaca_ws._run_forever())


if __name__ == "__main__":
    from dotenv import load_dotenv

    logger.info("Starting MainStreet")

    load_dotenv()
    ms = MainStreet(["AAPL", "AMZN"])
    ms.start()
