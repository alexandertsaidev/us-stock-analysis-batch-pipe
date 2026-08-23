import pandas as pd

from types import MappingProxyType

import logging

from .stock_indicator import StockIndicator
from .stock_indicator_explain import StockIndicatorExplain

logger = logging.getLogger(__name__)

class Analyzer:

    def __init__(
            self,
            df: pd.DataFrame,
            period: str,
            params: dict
        ):
        self.df = df
        self.period = period
        self.params = MappingProxyType(params)

        self.__run_one_period_one_ticker(
            df = self.df,
            period = self.period,
            params = self.params,
        )
        
    def __resample(
            self,
            df: pd.DataFrame,
            period: str
        ):

        df.set_index("Date", inplace=True, drop=False)

        try:
            ticker = df["ticker"].iloc[0]
            created_at = df["created_at"].iloc[0]
            
            if period == "D":

                df["period"] = period

            else:
                df = df.resample(period).agg({
                    'Open':   'first',
                    'High':   'max',
                    'Low':    'min',
                    'Close':  'last',
                    'Adj Close':  'last',
                    'Volume': 'sum',
                }).reset_index()

                df["period"] = period
                df["Date"]   = pd.to_datetime(df["Date"])
                df.set_index("Date", inplace=True, drop=False)
                df["ticker"] = ticker
                df["created_at"] = created_at

        except Exception as e:
            logger.error(f"處理 {ticker}, 週期: {period} 發生錯誤: {e}", exc_info=True)

        return df

    def __run_one_period_one_ticker(
            self,
            df: pd.DataFrame,
            period: str,
            params: dict
        ):

        # 1. resample
        df = self.__resample(df, period)

        # 2. indicator
        indicator = StockIndicator(
            df,
            params = params["indicator_params"]
        )
        
        # 3. explain
        explain = StockIndicatorExplain(
            indicator.df,
            params = params["explain_params"]
        )

        self.df = explain.df

        return



