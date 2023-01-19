import os
from util import (
    fibonacci,
    get_bars_for_symbol,
    get_env_var,
    get_logger,
    download_stock_data,
)
from datetime import datetime, timedelta
import asyncio
import logging
import time
from threading import Thread

from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed
from alpaca.trading.client import TradingClient
from alpaca.trading.stream import TradingStream
from finta import TA

import numpy as np
import pandas as pd

logger = get_logger("AIOTrader")


class AIOTrader:
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
        self.filled_rolling_bars = True
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

        # calculate indicators
        engineered = self._feature_engineer(
            get_bars_for_symbol(self.rolling_bars, b.symbol)
        )

        # make decision
        decision = self._make_decision(engineered)

        # act on decision

        # log
        logger.info(f"New bar\n{engineered}")

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

        # backtest
        self.backtest()

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
        self.trading_client.cancel_orders()  # cancel all orders
        self.trading_client.close_all_positions(
            cancel_orders=True
        )  # close any hanging positions
        self.alpaca_data_client.stop()
        self.trading_stream.stop()
        self.alpaca_data_client_thread.join()
        self.trading_stream_thread.join()

    def backtest(self) -> None:
        """Backtests the strategy with the same decision making function used live

        Steps for each symbol:
        0. Set up starting balances
        1. Download data
        2. Feature engineer
        3. Iterate through each bar
        4. Make decision
        5. Record decision and apply to running balance
        6. Assess performance w.r.t to starting balance AND holding
        """
        assert self.filled_rolling_bars == True, "Must fill rolling bars first"
        for symbol in self.symbols:
            # 0. Set up starting balances
            starting_balance = 10000
            running_balance = starting_balance
            holding_balance = starting_balance
            holding_shares = 0

            # 1. Download data
            bars = get_bars_for_symbol(self.rolling_bars, symbol)

            # 2. Feature engineer
            engineered = self._feature_engineer(bars)
            with_y = self._compute_y(engineered)

            # 3. Iterate through each bar
            for _, row in with_y.iterrows():
                # 4. Make decision
                decision = self._make_decision(row)

                # 5. Record decision and apply to running balance
                if decision == "buy":
                    running_balance -= row["close"]
                    holding_shares += 1
                elif decision == "sell":
                    running_balance += row["close"]
                    holding_shares -= 1

                # update holding balance
                holding_balance = holding_shares * row["close"]

    ## ------------------------------------------------------------------------

    ## private functions ------------------------------------------------------
    def _feature_engineer(self, bars: pd.DataFrame) -> pd.DataFrame:
        """Feature engineer the bars. Can be used to add indicators, etc. Length of DF will be checked

        Args:
            bars (pd.DataFrame): incoming bars

        Returns:
            pd.DataFrame: feature engineered bars
        """
        # make copy
        df = bars.copy()

        # add x indicators
        df["RSI"] = TA.RSI(df, 14)

        # add returns
        for r in fibonacci(7):
            df[f"return_{r}"] = np.log(df["close"] / df["close"].shift(r)).dropna()

        # drop na and return
        return df.dropna()

    def _compute_y(self, feature_engineered_bars: pd.DataFrame) -> pd.DataFrame:
        """Compute the y variable (decision column)

        Args:
            feature_engineered_bars (pd.DataFrame): bars with features

        Returns:
            pd.DataFrame: _description_
        """
        df = feature_engineered_bars.copy()
        df["y"] = np.where(df["close"].shift(1) > df["close"], 1, -1)
        print(df)
        return df.dropna()

    def _make_decision(self, bars: pd.DataFrame) -> str:
        """Makes a decision to sell or buy based on the bars"""
        pass

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

    logger.info("Starting AIOTrader")

    load_dotenv()
    ms = AIOTrader(["AAPL", "AMZN"])
    ms.start()
