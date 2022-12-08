import os
from datetime import datetime, timedelta, timezone
from math import floor, ceil
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
import logging


utc = ZoneInfo("UTC")
USING_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
USING_TIMEZONE = timezone.utc
DTFORMAT = "%Y-%m-%dT%H:%M:%SZ"

BAR_COLMAPS = {
    "t": "timestamp",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
}

logger = logging.getLogger(__name__)
load_dotenv()


def get_env_var(var_name: str) -> str:
    """Gets an environment variable"""
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise NameError(f"Can't find {var_name} in env!")


def resample_stock_data(df: pd.DataFrame, resample: str) -> pd.DataFrame:
    """Resample the stock data

    Args:
        df (pd.DataFrame): input ohlcv dataframe
        resample (str): resample time string. see pandas docs

    Returns:
        pd.DataFrame: resampled df
    """
    logic = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    df = (
        df.resample(f"{resample}", closed="left", label="left", origin="end")
        .apply(logic)
        .iloc[1:]  # because origin=end, first bar will be cut off
    )
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].fillna(
        method="ffill"
    )
    return df


def download_stock_data(
    symbol: str or list,
    start_time: datetime = datetime.now() - timedelta(hours=24),
    end_time: datetime = datetime.now(),
    fill_empty: bool = False,
    multi_index: bool = False,
    save: bool = False,
    save_path: str = "data",
) -> pd.DataFrame:
    """Use finnhub api to get historcal bars. Uses 1m bars by default.

    Args:
        symbol (str): [description]
        start_time (datetime, optional): [description]. Defaults to datetime.now()-timedelta(hours=24).
        end_time (datetime, optional): [description]. Defaults to datetime.now().
        fill_empty (bool, optional): Fill empty rows or not.
        multi_index (bool, optional): include symbol and timestamp as index

    Returns:
        pd.DataFrame: ohlcv candles
    """
    # needs to be in utc
    start = floor(start_time.astimezone(utc).timestamp())
    end = ceil(end_time.astimezone(utc).timestamp())
    token = os.getenv("FINNHUB_KEY_ID")
    assert token, "Missing Finnub key! Is 'FINNHUB_KEY_ID' in your .env?"

    parsed_symbols = symbol
    if isinstance(symbol, str):
        parsed_symbols = [symbol]

    total = pd.DataFrame()
    for s in list(parsed_symbols):
        try:
            url = (
                "https://finnhub.io/api/v1/stock/candle?"
                f"symbol={s}&resolution=1"
                f"&from={start}&to={end}&token={token}"
            )
            raw = json.loads(requests.get(url).text)
            df = pd.DataFrame(raw)
            df = df.rename(columns=BAR_COLMAPS)
            df["timestamp"] = df["timestamp"].apply(
                lambda x: datetime.fromtimestamp(x, tz=USING_TIMEZONE)
            )
            df = df.set_index("timestamp")

            if not all(df["s"] == "ok"):
                raise ValueError
            df = df.drop(columns=["s"])

            df = df.reindex(columns=["open", "high", "low", "close", "volume"])

            if fill_empty:
                df = resample_stock_data(df, "1T")

            if multi_index or isinstance(symbol, list):
                df["symbol"] = s
                df = df.reset_index()
                df = df.set_index(["timestamp", "symbol"])

            total = pd.concat([total, df])

            if save:
                os.makedirs(save_path, exist_ok=True)
                total.to_csv(
                    os.path.join(
                        save_path,
                        f"{s}-{start_time.strftime(DTFORMAT)}-{end_time.strftime(DTFORMAT)}-1m.csv",
                    )
                )

        except Exception as e:
            logger.error(
                f"Couldn't get bars for {s} from "
                f"{start_time} to {end_time} "
                f"with freq 1Min ({e})"
            )
            raise e

    return total
