import pandas as pd
import numpy as np

from types import MappingProxyType

import talib

class StockIndicator:

    def __init__(
            self,
            df: pd.DataFrame,
            params: dict
        ):

        self.df = df
        self.params = MappingProxyType(params)

        self.__calculate()

    def __calculate(self):

        if self.df.empty or not self.params :
            return
        
        open = self.df["Open"].values.astype(np.float64)
        high = self.df["High"].values.astype(np.float64)
        low = self.df["Low"].values.astype(np.float64)
        close = self.df["Close"].values.astype(np.float64)
        volume = self.df["Volume"].values.astype(np.float64)
    
        indicators_result = {}

        # ADX
        if "ADX" in self.params:
            tp = self.params["ADX"].get("timeperiod", 14)
            indicators_result["ADX"] = talib.ADX(high, low, close, timeperiod=tp)

        # MACD
        if "MACD" in self.params:
            tp = self.params["MACD"]
            macd, signal, hist = talib.MACD(
                close,
                fastperiod=tp.get("fastperiod", 12),
                slowperiod=tp.get("slowperiod", 26),
                signalperiod=tp.get("signalperiod", 9)
            )
            indicators_result["MACD"] = macd
            indicators_result["MACD_signal"] = signal
            indicators_result["MACD_hist"] = hist

        # PLUS_DI / MINUS_DI
        if "DI" in self.params:
            tp = self.params["DI"].get("timeperiod", 14)
            indicators_result["PLUS_DI"] = talib.PLUS_DI(high, low, close, timeperiod=tp)
            indicators_result["MINUS_DI"] = talib.MINUS_DI(high, low, close, timeperiod=tp)

        # STOCH
        if "STOCH" in self.params:
            tp = self.params["STOCH"]
            slowk, slowd = talib.STOCH(
                high, low, close,
                fastk_period=tp.get("fastk_period", 14),
                slowk_period=tp.get("slowk_period", 3),
                slowk_matype=tp.get("slowk_matype", 0),
                slowd_period=tp.get("slowd_period", 3),
                slowd_matype=tp.get("slowd_matype", 0)
            )
            indicators_result["STOCH_slowk"] = slowk
            indicators_result["STOCH_slowd"] = slowd

        # WILLR
        if "WILLR" in self.params:
            tp = self.params["WILLR"].get("timeperiod", 14)
            indicators_result["WILLR"] = talib.WILLR(high, low, close, timeperiod=tp)

        # Bollinger Bands (std dev : 2.7)
        if "BBANDS_27" in self.params:
            tp = self.params["BBANDS_27"]
            upper, middle, lower = talib.BBANDS(
                close,
                timeperiod=tp.get("timeperiod", 13),
                nbdevup=tp.get("nbdevup", 2.7),
                nbdevdn=tp.get("nbdevdn", 2.7),
                matype=tp.get("matype", 1)
            )
            indicators_result["upperband"] = upper
            indicators_result["middleband"] = middle
            indicators_result["lowerband"] = lower

        # Bollinger Bands (std dev : 1.7) 
        if "BBANDS_17" in self.params:
            tp = self.params["BBANDS_17"]
            upper_17, middle, lower_17 = talib.BBANDS(
                close,
                timeperiod=tp.get("timeperiod", 13),
                nbdevup=tp.get("nbdevup", 1.7),
                nbdevdn=tp.get("nbdevdn", 1.7),
                matype=tp.get("matype", 1)
            )
            indicators_result["upper_1_7"] = upper_17
            indicators_result["lower_1_7"] = lower_17

        # EMA
        if "EMA" in self.params:
            for tp in self.params["EMA"].get("ema_period", [13,26]):
                indicators_result[f"EMA{tp}"] = talib.EMA(close, timeperiod=tp)
        # SMA
        if "SMA" in self.params:
            for tp in self.params["SMA"].get("sma_period", [50,200]):
                indicators_result[f"SMA{tp}"] = talib.SMA(close, timeperiod=tp)

        # ATR
        if "ATR" in self.params:
            tp = self.params["ATR"].get("timeperiod", 14)
            indicators_result["ATR"] = talib.ATR(high, low, close, timeperiod=tp)

        # FI
        if "FI" in self.params:
            fi_periods = self.params["FI"].get("ema_period", [2,13])
            
            # 與前一天比較
            diff = np.empty_like(close)
            diff[0] = 0
            diff[1:] = close[1:] - close[:-1]
            
            # 原始 Force Index
            fi_raw = diff * volume
            indicators_result["FI"] = fi_raw
            
            # 平滑 FI
            for tp in fi_periods:
                indicators_result[f"FI_{tp}"] = talib.EMA(fi_raw, timeperiod=tp)

        # Bull & Bear Power
        # BullPower = High - EMA13
        # BearPower = Low - EMA13
        if "Bull_Bear_Power" in self.params:
            tp = self.params["Bull_Bear_Power"].get("ema_period", 13)
            ema = talib.EMA(close, timeperiod=tp)
            indicators_result["Bull_Power"] = high - ema
            indicators_result["Bear_Power"] = low - ema

        # # 加入 DataFrame
        # for key, value in indicators_result.items():
        #     self.df[key] = value

        indicators_df = pd.DataFrame(indicators_result, index=self.df.index)
        self.df = pd.concat([self.df, indicators_df], axis=1)
        
        return
