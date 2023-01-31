"""Holds the model for prediction
"""
import joblib
from util import fibonacci, get_logger
import numpy as np
import pandas as pd
from finta import TA
from sklearn.neighbors import KNeighborsRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

logger = get_logger(__name__)


class TertiaryModel:
    """Model class for prediction"""

    def __init__(self):
        self.knn_model = KNeighborsRegressor(n_neighbors=3)
        self.accuracy = 0

    def train(self, X_and_y: pd.DataFrame, tt_split: float = 0.8) -> float:
        """Train the model on the given data

        Args:
            X (pd.DataFrame): feature engineered bars
            y (pd.DataFrame): y variable
        """
        logger.warning("No training implemented")
        X = X_and_y.drop("y", axis=1)
        y = X_and_y["y"]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=1 - tt_split
        )
        self.knn_model.fit(X_train, y_train)
        y_pred = np.round(self.knn_model.predict(X_test))  # round to binary
        self.accuracy = accuracy_score(y_test, y_pred)
        logger.info(f"Accuracy: {self.accuracy}")
        return self.accuracy

    @staticmethod
    def feature_engineer(bars: pd.DataFrame) -> pd.DataFrame:
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
        df["SMA_20"] = TA.SMA(df, 20)
        df["SMA_50"] = TA.SMA(df, 50)

        # add returns
        for r in fibonacci(7):
            df[f"return_{r}"] = np.log(df["close"] / df["close"].shift(r)).dropna()

        # drop na and return
        return df.dropna()

    @staticmethod
    def compute_y(feature_engineered_bars: pd.DataFrame) -> pd.DataFrame:
        """Compute the y variable (decision column)

        Args:
            feature_engineered_bars (pd.DataFrame): bars with features

        Returns:
            pd.DataFrame: _description_
        """
        df = feature_engineered_bars.copy()
        df["y"] = np.where(df["close"].shift(1) > df["close"], 1, -1)
        return df.dropna()

    def make_decision(self, feature_engineered_bars: pd.DataFrame) -> int:
        """Makes a decision to sell or buy based on the bars
        1 = buy
        -1 = sell
        0 = hold
        """

        latest = feature_engineered_bars.tail(1)  # get latest row
        pred = self.knn_model.predict(latest)
        return pred

    def save(self, path):
        """Saves the model to the given path"""
        joblib.dump(self, path)

    @staticmethod
    def load(path):
        """Loads the model from the given path"""
        return joblib.load(path)
