# engine/multi_period_analyzer.py

import pandas as pd

from .indicator_engine import IndicatorEngine
from .explain_engine import ExplainEngine

class MultiPeriodAnalyzer:

    def __init__(self):
        pass

    def run_single_period_with_ticker(
            self,
            df: pd.DataFrame,
            period: str, 
            config: dict
        ):

        # 1. resample
        df_period = self._resample(df, period)

        # 2. indicator
        df_period = IndicatorEngine(
            df_period,
            params=config["indicator_params"]
        ).calculate()
        
        # 3. explain
        df_period = ExplainEngine(
            df_period,
            params=config["explain_params"]
        ).mark_all()
        
        return df_period

    def _resample(self, df: pd.DataFrame, period: str):

        # ── 確保 index 是 DatetimeIndex，相容 date32 / object / string ──
        if not isinstance(df.index, pd.DatetimeIndex):
            df = df.copy()
            df.index = pd.to_datetime(df.index)

            try:
                ticker = df["ticker"].iloc[0]
                created_at = df["created_at"].iloc[0]

                if period == "D":
                    period_data = df
                    period_data["period"] = period

                else:
                    period_data = df.resample(period).agg({
                        'Open':   'first',
                        'High':   'max',
                        'Low':    'min',
                        'Close':  'last',
                        'Adj Close':  'last',
                        'Volume': 'sum',
                    }).reset_index()

                    period_data["period"] = period
                    period_data['Date']   = pd.to_datetime(period_data['Date'])
                    period_data.set_index("Date", inplace=True, drop=False)
                    period_data["ticker"] = ticker
                    period_data["created_at"] = created_at

            except Exception as e:
                raise ValueError(f"Unsupported period: {period}, error: {e}")

        return period_data


