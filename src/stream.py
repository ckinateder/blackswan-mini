import os, websocket
from util import get_env_var
from alpaca_trade_api.common import URL
from alpaca_trade_api.stream import Stream


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

        # open finnhub websocket
        self.finnhub_ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={self.FINNHUB_KEY_ID}",
            on_message=self._on_finnhub_message,
            on_error=self._on_finnhub_error,
            on_close=self._on_finnhub_close,
        )
        self.finnhub_ws.on_open = self._on_finnhub_open

        # open alpaca websocket
        self.alpaca_ws = Stream(
            self.APCA_API_KEY_ID,
            self.APCA_API_SECRET_KEY,
            base_url=URL(self.APCA_API_BASE_URL),
            data_feed="iex",
        )  # <- replace to 'sip' if you have PRO subscription

    async def trade_callback(t):
        print("trade", t)

    async def quote_callback(q):
        print("quote", q)

    def _on_finnhub_close(self, ws, *args) -> None:
        """Finnhub websocket close"""
        print("Finnhub websocket closed")

    def _on_finnhub_error(self, ws, error) -> None:
        """Finnhub websocket error"""
        print(error)

    def _on_finnhub_open(self, ws) -> None:
        """Finnhub websocket open"""
        print("Finnhub websocket open")
        for symbol in self.symbols:
            ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')
            print(f"Subscribed to {symbol}")

    def _on_finnhub_message(self, ws, message) -> None:
        """Finnhub websocket message"""
        print(message)

    def start(self) -> None:
        """Starts the websocket"""
        self.finnhub_ws.run_forever()


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    ms = MainStreet(["AAPL", "AMZN", "BINANCE:BTCUSDT", "IC MARKETS:1"])
    ms.start()
