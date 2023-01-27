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
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, TradeEvent, OrderType

from finta import TA
from model import TertiaryModel

import numpy as np
import pandas as pd

logger = get_logger("AIOTrader")

# configs
MIN_ROI = 1.03  # min ROI for trading

# constants
SIDE_CONVERTER = {1: OrderSide.BUY, -1: OrderSide.SELL}


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

        # define rolling dataframe - this keeps track of all bars
        self._fill_rolling_bars()
        logger.info("Filled rolling bars")

        # set stop flag
        self.start_time = None
        self.end_time = None

        # set trading flags
        self.trading = {s: False for s in self.symbols}

        # init models
        self.models = {s: TertiaryModel() for s in self.symbols}

        # init account
        self.account = self.trading_client.get_account()

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

        # check if not trading
        if not self.trading[b.symbol]:
            logger.debug(
                f"Skipping trading for {b.symbol} (reason: backtesting failed)"
            )
            return

        # calculate indicators
        engineered = TertiaryModel.feature_engineer(
            get_bars_for_symbol(self.rolling_bars, b.symbol)
        )

        # make decision
        decision = self.models[b.symbol].make_decision(engineered)
        logger.info(f"Decision for {b.symbol} @ close ${b.close:,.2f}: {decision}")
        if decision == 0:
            return

        # make order
        limit_order_data = LimitOrderRequest(
            symbol=b.symbol,
            limit_price=round(b.close, 2),
            qty=1,
            side=SIDE_CONVERTER[decision],
            time_in_force=TimeInForce.DAY,
        )

        # Limit order
        limit_order = self.trading_client.submit_order(order_data=limit_order_data)

        # act on decision

        # log
        logger.info(f"New bar for {b.symbol}\n{engineered}")

    async def _on_trade(self, t: any):
        """Will receive a trade from the websocket

        Args:
            t (any): incoming trade
        """
        o = t.order
        if t.event == TradeEvent.FILL:
            logger.info(f"Trade filled: {o.id} ({o.side} {o.qty} @ {o.filled_at})")
        elif t.event == TradeEvent.NEW:
            deets = f"{o.side} {o.qty}"
            if o.type == OrderType.LIMIT:
                deets += f" @ {o.limit_price}"
            logger.info(f"Trade created: {o.id} ({deets})")
        elif t.event == TradeEvent.CANCELED:
            deets = f"{o.side} {o.qty}"
            if o.type == OrderType.LIMIT:
                deets += f" @ {o.limit_price}"
            logger.info(f"Trade canceled: {o.id} ({deets})")
        else:
            logger.info(f"Unrecognized trade: {o.id}")

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
            logger.info("Refilling rolling bars")
            self._fill_rolling_bars()

        self.start_time = datetime.now()

        # start threads
        self.alpaca_data_client_thread = Thread(target=self.alpaca_data_client.run)
        self.alpaca_data_client_thread.start()
        self.trading_stream_thread = Thread(target=self.trading_stream.run)
        self.trading_stream_thread.start()
        self.account_monitor_thread = Thread(target=self._account_monitor)
        self.account_monitor_thread.start()

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
        logger.info("Stopping")
        self.end_time = datetime.now()
        self.trading_client.cancel_orders()  # cancel all orders
        self.trading_client.close_all_positions(
            cancel_orders=True
        )  # close any hanging positions
        logger.info("Closing all positions and canceling all orders")
        self.alpaca_data_client.stop()
        self.trading_stream.stop()
        self.alpaca_data_client_thread.join()
        self.trading_stream_thread.join()
        self.account_monitor_thread.join()

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
            engineered = TertiaryModel.feature_engineer(bars)
            with_y = TertiaryModel.compute_y(engineered)

            # 2a. Train model for each symbol

            ####

            # 3. Iterate through each bar
            for _, row in with_y.iterrows():
                # 4. Make decision
                decision = self.models[symbol].make_decision(
                    row.to_frame().T
                )  # must be a dataframe NOT series

                # 5. Record decision and apply to running balance
                if decision == 1:
                    running_balance -= row["close"]
                    holding_shares += 1
                elif decision == -1:
                    running_balance += row["close"]
                    holding_shares -= 1

                # update holding balance
                holding_balance = holding_shares * row["close"]

            # 6. Assess performance w.r.t to starting balance AND holding
            # final rebalance
            running_balance += holding_shares * row["close"]
            roi = 1 + ((running_balance - starting_balance) / starting_balance)

            logger.info(
                f"{symbol} backtest results:\n\tStarting balance: ${starting_balance:,.2f}"
                f"\n\tEnding balance: ${running_balance:,.2f} (selling off {holding_shares} shares at end), If Buy and Hold, balance: ${holding_balance:,.2f} & {holding_shares} shares"
                f"\n\tROI: {roi:.4}"
            )
            if roi > MIN_ROI:
                logger.info(f"Enabling trading for {symbol}")
                self.trading[symbol] = True
            else:
                logger.info(f"Disabling trading for {symbol} :(")
                self.trading[symbol] = False

    ## ------------------------------------------------------------------------

    ## private functions ------------------------------------------------------
    def _get_market_clock(self) -> any:
        """Gets the market clock"""
        return self.trading_client.get_clock()

    def _close_to_market_close(self) -> bool:
        """Returns true if 5 minutes from market close

        Returns:
            bool: if 5 mins from market close
        """
        clk = self._get_market_clock()
        return (clk.next_close - clk.timestamp).total_seconds() < 300

    def _fill_rolling_bars(self) -> None:
        """Fills the rolling bars with data"""
        self.rolling_bars = download_stock_data(
            self.symbols,
            start_time=datetime.now() - timedelta(days=5),
            end_time=datetime.now(),
            fill_empty=True,
        )
        self.filled_rolling_bars = True

    def _account_monitor(self) -> None:
        """Gets account balance every 60 seconds"""
        every = 60
        while self.end_time == None:
            t = datetime.utcnow()
            st = t.second + (t.microsecond / 1000000.0)
            time.sleep(every - st)
            self.account = self.trading_client.get_account()
            logger.info(f"account: {self.account}")

    ## ------------------------------------------------------------------------


if __name__ == "__main__":
    from dotenv import load_dotenv

    logger.info("Starting AIOTrader")

    load_dotenv()
    ms = AIOTrader(["AAPL", "AMZN"])
    ms.start()
