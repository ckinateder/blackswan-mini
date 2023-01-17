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
from alpaca.trading.stream import TradingStream
from threading import Thread

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
        self.alpaca_data_client = StockDataStream(
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
        self.trading_stream = TradingStream(
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

        # set stop flag
        self.stop_flag = False

        # subscribe to symbols and trades
        self.trading_stream.subscribe_trade_updates(self._on_trade)
        logger.info(f"Subscribed to trade status updates")
        for s in self.symbols:
            self.alpaca_data_client.subscribe_bars(self._on_bar, s)
            logger.info(f"Subscribed to {s} bars")

    ## async handlers ---------------------------------------------------------
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
        # self.alpaca_data_client.stop()
        logger.info(f"New bar\n{self.rolling_bars}")

    async def _on_trade(self, t: any):
        """Will receive a trade from the websocket

        Args:
            t (any): incoming trade
        """
        print(t)

    ## ------------------------------------------------------------------------

    ## calling functions
    def start(self) -> None:
        """Starts the websocket - BLOCKING"""
        # sleep until time to open
        clk = self._get_market_clock()
        if not clk.is_open:
            duration = clk.next_open - clk.timestamp
            logger.info(f"Sleeping {duration} until market open")
            time.sleep(duration.total_seconds())

        # start websockets
        self.alpaca_data_client_thread = Thread(target=self.alpaca_data_client.run)
        self.alpaca_data_client_thread.start()
        self.trading_stream_thread = Thread(target=self.trading_stream.run)
        self.trading_stream_thread.start()

        # sleep until time to close
        while not self._close_to_market_close():
            try:
                time.sleep(60)
            except KeyboardInterrupt:
                break

        # close websockets
        self.stop()

    def stop(self) -> None:
        """Stops the running process"""
        self.stop_flag = True
        self.alpaca_data_client.stop()
        self.trading_stream.stop()
        self.alpaca_data_client_thread.join()
        self.trading_stream_thread.join()

    ## ------------------------------------------------------------------------

    ## private functions ------------------------------------------------------

    def _get_market_clock(self) -> any:
        return self.trading_client.get_clock()

    def _close_to_market_close(self) -> bool:
        """Returns true if 5 minutes from market close

        Returns:
            bool: if 5 mins from market close
        """
        clk = self._get_market_clock()
        return (clk.next_close - clk.timestamp).total_seconds() < 300

    ## ------------------------------------------------------------------------


if __name__ == "__main__":
    from dotenv import load_dotenv

    logger.info("Starting MainStreet")

    load_dotenv()
    ms = MainStreet(["AAPL", "AMZN"])
    ms.start()
