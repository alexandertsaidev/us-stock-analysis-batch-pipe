import pandas as pd
import numpy as np

from types import MappingProxyType

from scipy.signal import find_peaks

import heapq

class StockIndicatorExplain:
    def __init__(
            self,
            df: pd.DataFrame,
            params: dict
        ):

        self.df = df.reset_index(drop=True)
        
        # default_params.update(params)
        self.params = MappingProxyType(params)

        self.__mark_all()

    def __mark_EMA_bullbear(self):
        ema_short = self.params["EMA_short"]
        ema_long  = self.params["EMA_long"]

        trend = ["Up", "Down"]
        # 1.
        ema_diff = self.df[f"EMA{ema_short}"] - self.df[f"EMA{ema_long}"]
        conditions_1 = [
            ((ema_diff > 0) & (ema_diff.shift(1) < 0)),
            ((ema_diff < 0) & (ema_diff.shift(1) > 0))
        ]
        self.df["EMA_bullbear"] = np.select(conditions_1, trend, default=None)

        # 2.
        self.df[f"EMA{ema_short}_slope"] = self.df[f"EMA{ema_short}"].diff()
        conditions_2 = [
            (self.df[f"EMA{ema_short}_slope"] > 0),
            (self.df[f"EMA{ema_short}_slope"] < 0)
        ]
        self.df[f"EMA{ema_short}_trend"] = np.select(conditions_2, trend, default=None)

        return

    def __mark_price_peaks_troughs_detail(self):
        minima_price_gap = self.params["minima_price_gap"]
        minima_price_prominence  = self.params["minima_price_prominence"]
        input_price_1  = self.params["input_price_1"]
        search_quantity  = self.params["search_quantity"]

        prices = self.df[input_price_1].values

        peak_idx, _ = find_peaks(
            prices,
            distance = minima_price_gap,
            prominence = minima_price_prominence
        )

        trough_idx, _ = find_peaks(
            -prices,
            distance = minima_price_gap,
            prominence = minima_price_prominence
        )

        self.df["price_Peak"] = False
        self.df["price_Trough"] = False

        self.df.loc[peak_idx, "price_Peak"] = True
        self.df.loc[trough_idx, "price_Trough"] = True

        self.df["Higher_Peak"] = False
        self.df["Lower_Trough"] = False
        self.df["price_search"] = None

        # 找出「所有被標記為峰頂」的 index 位置
        peak_indices = self.df.index[self.df["price_Peak"] == True].tolist()

        for i in range(search_quantity, len(peak_indices)) :

            # 取 input_price_1 對應的 ATR 作為價格差容忍
            max_diff = self.df.loc[peak_indices[i], "ATR"]

            for j in range(i - search_quantity , i):

                diff = abs(self.df.loc[peak_indices[i], input_price_1]
                           - self.df.loc[peak_indices[j], input_price_1])
                
                if ((diff <= max_diff) and
                    (self.df.loc[peak_indices[i], input_price_1] > self.df.loc[peak_indices[j], input_price_1]) ):

                    earliest_index = j
                    self.df.loc[peak_indices[i], "Higher_Peak"] = True
                    self.df.loc[peak_indices[i], "price_search"] = peak_indices[earliest_index]
                    break

        # 找出「所有被標記為低點」的 index 位置
        trough_indices = self.df.index[self.df["price_Trough"] == True].tolist()

        for i in range(search_quantity, len(trough_indices)) :

            # 取 input_price_1 對應的 ATR 作為價格差容忍
            max_diff = self.df.loc[trough_indices[i], "ATR"]

            for j in range(i - search_quantity , i):

                diff = abs(self.df.loc[trough_indices[i], input_price_1]
                           - self.df.loc[trough_indices[j], input_price_1])
                
                if ((diff <= max_diff) and
                    (self.df.loc[trough_indices[i], input_price_1] < self.df.loc[trough_indices[j], input_price_1]) ):

                    earliest_index = j
                    self.df.loc[trough_indices[i], "Lower_Trough"] = True
                    self.df.loc[trough_indices[i], "price_search"] = trough_indices[earliest_index]
                    break

        return

    def __mark_FI_2_peaks_troughs_detail(self):
        minima_FI_2_gap = self.params["minima_FI_2_gap"]
        minima_FI_2_prominence  = self.params["minima_FI_2_prominence"]

        FI_values = self.df["FI_2"].values

        peak_idx, _ = find_peaks(
            FI_values,
            distance = minima_FI_2_gap,
            prominence = minima_FI_2_prominence
        )
        trough_idx, _ = find_peaks(
            -FI_values,
            distance = minima_FI_2_gap,
            prominence = minima_FI_2_prominence
        )

        # 過濾：peak 只保留正值，trough 只保留負值
        peak_idx = peak_idx[FI_values[peak_idx] > 0]
        trough_idx = trough_idx[FI_values[trough_idx] < 0]

        self.df["FI_2_Peak"] = False
        self.df["FI_2_Trough"] = False
        self.df.loc[peak_idx, "FI_2_Peak"] = True
        self.df.loc[trough_idx, "FI_2_Trough"] = True

        return

    def __mark_FI_13_peaks_troughs_detail(self):

        FI_13_threshold = self.params["indicators"]["Side_1"]["FI_13"]["threshold"]

        self.df["FI_13_diff"] = self.df["FI_13"] - self.df["FI_13"].shift(1) 

        peak_cond = (
            (self.df["FI_13"] > 0) &
            (self.df["FI_13_diff"] > 0) &
            (abs(self.df["FI_13_diff"] / self.df["FI_13"].shift(1) ) * 100 >= FI_13_threshold)
        )

        trough_cond = (
            (self.df["FI_13"] < 0) &
            (self.df["FI_13_diff"] < 0) &
            (abs(self.df["FI_13_diff"] / self.df["FI_13"].shift(1) ) * 100 >= FI_13_threshold)
        )

        self.df[f"FI_13_Peak"] = peak_cond
        self.df[f"FI_13_Trough"] = trough_cond

        return

    def __mark_upper_lower_band(self):

        self.df["gt_upper"]   = (self.df["Close"] >= self.df["upperband"])
        self.df["near_upper"] = ((self.df["Close"] < self.df["upperband"]) & (self.df["Close"] >= self.df["upper_1_7"]))
        self.df["lt_lower"]   = (self.df["Close"] <= self.df["lowerband"])
        self.df["near_lower"] = ((self.df["Close"] > self.df["lowerband"]) & (self.df["Close"] <= self.df["lower_1_7"]))
        
        return

    def __mark_trend_1(self):
        
        self.df["MACD_hist_trend"] = None
        self.df["FI_13_trend"] = None
        self.df["MACD_hist_trend_pair"] = None
        self.df["FI_13_trend_pair"] = None

        # 峰頂標記欄位名稱（字串），"Higher_Peak"=True 的列才參與 Down 判斷
        higher_peak_indices = self.df.index[self.df["Higher_Peak"] == True].tolist()

        for i in range(len(higher_peak_indices)) :
            search_index = self.df.loc[higher_peak_indices[i], "price_search"]

            for indicator, value in self.params["indicators"]["Side_1"].items() :

                max_condition = float('-inf')

                for j in range(search_index, higher_peak_indices[i]) :
                
                    if self.df.loc[higher_peak_indices[i], indicator] < self.df.loc[j, indicator] :

                        condition = abs(self.df.loc[higher_peak_indices[i], indicator] - self.df.loc[j, indicator]) \
                                    /abs(self.df.loc[j, indicator]) *100

                        if condition >= value["threshold"] and condition > max_condition:
                            max_condition = condition
                            self.df.loc[higher_peak_indices[i], f"{indicator}_trend"] = "Down"
                            self.df.loc[higher_peak_indices[i], f"{indicator}_trend_pair"] = f"{j}_{higher_peak_indices[i]}"
                            
        # 谷底標記欄位名稱（字串），"Lower_Peak" =True 的列才參與 Up 判斷
        lower_trough_indices = self.df.index[self.df["Lower_Trough"] == True].tolist()
        
        for i in range(len(lower_trough_indices)) :
            search_index = self.df.loc[lower_trough_indices[i], "price_search"]

            for indicator, value in self.params["indicators"]["Side_1"].items() :

                max_condition = float('-inf')

                for j in range(search_index, lower_trough_indices[i]) :
                
                    if self.df.loc[lower_trough_indices[i], indicator] > self.df.loc[j, indicator] :

                        condition = abs(self.df.loc[lower_trough_indices[i], indicator] - self.df.loc[j, indicator]) \
                                    /abs(self.df.loc[j, indicator]) *100
                        
                        if condition >= value["threshold"] and condition > max_condition:
                            max_condition = condition
                            self.df.loc[lower_trough_indices[i], f"{indicator}_trend"] = "Up"
                            self.df.loc[lower_trough_indices[i], f"{indicator}_trend_pair"] = f"{j}_{lower_trough_indices[i]}"

        return
    
    def __mark_trend_2(self):
        pass

    def __mark_trend_3(self):
        
        self.df["Bull_Power_trend"] = None
        self.df["Bear_Power_trend"] = None
        self.df["Bull_Power_trend_pair"] = None
        self.df["Bear_Power_trend_pair"] = None

        # 峰頂標記欄位名稱（字串），"Higher_Peak"=True 的列才參與 Down 判斷
        higher_peak_indices = self.df.index[self.df["Higher_Peak"] == True].tolist()

        for i in range(len(higher_peak_indices)) :
            search_index = self.df.loc[higher_peak_indices[i], "price_search"]

            conditions = []
            for j in range(search_index, higher_peak_indices[i]) :

                if self.df.loc[j, "Bull_Power"] > 0 :
                    conditions.append( (j, abs(self.df.loc[j, "Bull_Power"])) )
            
            if conditions :
                top2 = heapq.nlargest(2, conditions, key=lambda x: x[1])

                if len(top2) >= 2 and top2[0][0] < top2[1][0] :

                    self.df.loc[higher_peak_indices[i], "Bull_Power_trend"] = "Down"
                    self.df.loc[higher_peak_indices[i], "Bull_Power_trend_pair"] = f"{top2[0][0]}_{top2[1][0]}"

        # 谷底標記欄位名稱（字串），"Lower_Peak" =True 的列才參與 Up 判斷
        lower_trough_indices = self.df.index[self.df["Lower_Trough"] == True].tolist()
        
        for i in range(len(lower_trough_indices)) :
            search_index = self.df.loc[lower_trough_indices[i], "price_search"]

            conditions = []
            for j in range(search_index, lower_trough_indices[i]) :

                if self.df.loc[j, "Bear_Power"] < 0 :
                    conditions.append( (j, abs(self.df.loc[j, "Bear_Power"])) )
            
            if conditions :
                top2 = heapq.nlargest(2, conditions, key=lambda x: x[1])

                if len(top2) >= 2 and top2[0][0] < top2[1][0] :

                    self.df.loc[lower_trough_indices[i], "Bear_Power_trend"] = "Up"
                    self.df.loc[lower_trough_indices[i], "Bear_Power_trend_pair"] = f"{top2[0][0]}_{top2[1][0]}"

        return

    def __mark_Long_Short_1(self):
        """
        對應的 _trend 欄位標記 Long / Short
        
        標記邏輯：
        - trend_cols 所有欄位同時為 "Up"   -> side_pair_1 = "Long"
        - trend_cols 所有欄位同時為 "Down" -> side_pair_1 = "Short"
        - 其餘                             -> side_pair_1 = None
        """
        
        # 先建立欄位，全部填 None
        self.df["Side_1"] = None

        # 取得對應的 _trend 欄位名稱
        trend_cols = [
            f"{indicator}_trend"
            for indicator in self.params["indicators"]["Side_1"].keys()
        ]

        # 條件：trend_cols 所有欄位同時為 "Up"
        Long_cond  = self.df[trend_cols].eq("Up").all(axis=1)
        
        # 條件：trend_cols 所有欄位同時為 "Down"
        Short_cond = self.df[trend_cols].eq("Down").all(axis=1)
        
        # 標記 Long / Short
        self.df.loc[Long_cond,  "Side_1"] = "Long"
        self.df.loc[Short_cond, "Side_1"] = "Short"
        
        return

    def __mark_Long_Short_2(self):

        # 取得對應的 _trend 欄位名稱

        conditions = [
            (self.df["EMA13_trend"] == "Up")   & (self.df["FI_2_Trough"] == True),
            (self.df["EMA13_trend"] == "Down") & (self.df["FI_2_Peak"]   == True)
        ]
        choices = ["Long", "Short"]

        self.df["Side_2"] = np.select(conditions, choices, default=None)

        return

    def __mark_Long_Short_3(self):

        conditions = [
            (self.df["Bear_Power_trend"] == "Up") & (self.df["Lower_Trough"] == True),
            (self.df["Bull_Power_trend"] == "Down") & (self.df["Higher_Peak"] == True)
        ]
        choices = ["Long", "Short"]

        self.df["Side_3"] = np.select(conditions, choices, default=None)

        return

    def __mark_all(self):
        steps = [
            self.__mark_EMA_bullbear,
            self.__mark_price_peaks_troughs_detail,
            self.__mark_FI_2_peaks_troughs_detail,
            self.__mark_FI_13_peaks_troughs_detail,
            self.__mark_upper_lower_band,
            self.__mark_trend_1,
            self.__mark_trend_3,
            self.__mark_Long_Short_1,
            self.__mark_Long_Short_2,
            self.__mark_Long_Short_3
        ]
        for step in steps:
            step()

        return

