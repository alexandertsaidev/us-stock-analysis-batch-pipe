# engine/indicator_engine.py

import pandas as pd
import numpy as np

import talib

class IndicatorEngine:

    def __init__(self,
                 df: pd.DataFrame, 
                 params: dict):
        
        self.df = df
        self.params = params
    
    def calculate(self):
        """
        對 DataFrame df 計算多個技術指標並新增欄位
        df 需包含至少 "Open","High","Low","Close","Volume"
        
        self.params: 指標參數字典，例如
            self.params = {
                "ADX": {"timeperiod":14},
                "MACD": {"fastperiod":12, "slowperiod":26, "signalperiod":9},
                "STOCH": {"fastk_period":14, "slowk_period":3, "slowk_matype":0, "slowd_period":3, "slowd_matype":0},
                "BBANDS": {"timeperiod":20, "nbdevup":2, "nbdevdn":2},
                "ATR": {"timeperiod":14},
                "EMA": {"periods":[13,26]},
                "FI": {"periods":[1,2,13]}
            }
        """
        if self.params is None:
            self.params = {}

        open = self.df["Open"].values.astype(np.float64)
        high = self.df["High"].values.astype(np.float64)
        low = self.df["Low"].values.astype(np.float64)
        close = self.df["Close"].values.astype(np.float64)
        volume = self.df["Volume"].values.astype(np.float64)
    
        result = {}

        # ADX
        if "ADX" in self.params:
            tp = self.params["ADX"].get("timeperiod", 14)
            result["ADX"] = talib.ADX(high, low, close, timeperiod=tp)

        # MACD
        if "MACD" in self.params:
            tp = self.params["MACD"]
            macd, signal, hist = talib.MACD(
                close,
                fastperiod=tp.get("fastperiod", 12),
                slowperiod=tp.get("slowperiod", 26),
                signalperiod=tp.get("signalperiod", 9)
            )
            result["MACD"] = macd
            result["MACD_signal"] = signal
            result["MACD_hist"] = hist

        # PLUS_DI / MINUS_DI
        if "DI" in self.params:
            tp = self.params["DI"].get("timeperiod", 14)
            result["PLUS_DI"] = talib.PLUS_DI(high, low, close, timeperiod=tp)
            result["MINUS_DI"] = talib.MINUS_DI(high, low, close, timeperiod=tp)

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
            result["STOCH_slowk"] = slowk
            result["STOCH_slowd"] = slowd

        # WILLR
        if "WILLR" in self.params:
            tp = self.params["WILLR"].get("timeperiod", 14)
            result["WILLR"] = talib.WILLR(high, low, close, timeperiod=tp)

        # Bollinger Bands
        if "BBANDS" in self.params:
            tp = self.params["BBANDS"]
            upper, middle, lower = talib.BBANDS(
                close,
                timeperiod=tp.get("timeperiod", 13),
                nbdevup=tp.get("nbdevup", 2.7),
                nbdevdn=tp.get("nbdevdn", 2.7),
                matype=tp.get("matype", 1)
            )
            result["upperband"] = upper
            result["middleband"] = middle
            result["lowerband"] = lower

        # Bollinger (std dev : 1.7) Bands
        if "SD_17" in self.params:
            tp = self.params["SD_17"]
            upper_17, middle, lower_17 = talib.BBANDS(
                close,
                timeperiod=tp.get("timeperiod", 13),
                nbdevup=tp.get("nbdevup", 1.7),
                nbdevdn=tp.get("nbdevdn", 1.7),
                matype=tp.get("matype", 1)
            )
            result["upper_1_7"] = upper_17
            result["lower_1_7"] = lower_17

        # EMA
        if "EMA" in self.params:
            for tp in self.params["EMA"].get("ema_period", [13,26]):
                result[f"EMA{tp}"] = talib.EMA(close, timeperiod=tp)
        # SMA
        if "SMA" in self.params:
            for tp in self.params["SMA"].get("sma_period", [50,200]):
                result[f"SMA{tp}"] = talib.SMA(close, timeperiod=tp)

        # ATR
        if "ATR" in self.params:
            tp = self.params["ATR"].get("timeperiod", 14)
            result["ATR"] = talib.ATR(high, low, close, timeperiod=tp)

        # ----- FI -----
        if "FI" in self.params:
            fi_periods = self.params["FI"].get("ema_period", [2,13])
            
            # 與前一天比較
            diff = np.empty_like(close)
            diff[0] = 0
            diff[1:] = close[1:] - close[:-1]
            
            # 原始 Force Index
            fi_raw = diff * volume
            result["FI"] = fi_raw
            
            # 平滑 FI
            for tp in fi_periods:
                result[f"FI_{tp}"] = talib.EMA(fi_raw, timeperiod=tp)

        # Bull / Bear Power
        # BullPower = High - EMA13
        # BearPower = Low - EMA13
        if "Bull_Bear_Power" in self.params:
            tp = self.params["Bull_Bear_Power"].get("ema_period", 13)
            ema = talib.EMA(close, timeperiod=tp)
            result["Bull_Power"] = high - ema
            result["Bear_Power"] = low - ema

        # 加入 DataFrame
        for key, value in result.items():
            self.df[key] = value

        return self.df
