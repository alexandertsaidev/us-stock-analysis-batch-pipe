import numpy as np

from scipy.signal import find_peaks

import heapq

class ExplainEngine:
    _DEFAULT_CONFIG = {
        "ema_short": 13,
        "ema_long": 26,
        "input_price_1": "Close",
        "extrema_price_gap": 5,
        "extrema_FI_2_gap": 5,
        "extrema_price_prominence": 1,
        "extrema_FI_2_prominence": 5,
        "search_quantity": 2,
        "indicator_pair_1": ["MACD_hist", "FI_13"],
        "indicator_pair_2": ["EMA13", "FI_2"],
        "indicator_pair_3": ["Bull_Power", "Bear_Power"],
        "pair_1_threshold": [30,70],
        "side_pair_1": "Side_1",
        "side_pair_2": "Side_2",
        "side_pair_3": "Side_3"
        }
    
    def __init__(self, df, **params):
        """
        初始化物件，並將傳入的 DataFrame 與配置參數設置到實例中。

        Parameters
        ----------
        df : pd.DataFrame
            股票或時間序列資料，初始化時會重設索引並建立副本，以避免修改原始 DataFrame。

        **kwargs : dict, optional
            可選的配置參數，用來覆蓋 DEFAULT_CONFIG 的預設值。
            每個 key-value 會被設置成物件屬性，例如：
                - self.param1 = kwargs.get('param1', DEFAULT_CONFIG['param1'])
            支持任意額外參數，方便靈活擴展。

        Behavior
        --------
        1. 重設 df 索引 (drop=True)，保證索引為 0,1,2,...。
        2. 建立 df 的副本，避免原始資料被修改。
        3. 生成 config 字典，將 DEFAULT_CONFIG 與 kwargs 合併。
        4. 將 config 中的每個 key 設置為物件屬性 (self.key = value)。
        """
        # 將 df 的索引重置並建立副本
        self.df = df.reset_index(drop=True).copy()

        # 複製預設配置，避免修改原始 DEFAULT_CONFIG
        config = self._DEFAULT_CONFIG.copy()

        # 更新配置，將 kwargs 中的值覆蓋預設
        config.update(params)

        # 將所有配置設定成物件屬性
        for key, value in config.items():
            setattr(self, key, value)

    def _mark_EMA_bullbear(self):

        self.df = self.df.copy()

        ema_diff = self.df[f"EMA{self.ema_short}"] - self.df[f"EMA{self.ema_long}"]
        conditions_1 = [
            ((ema_diff > 0) & (ema_diff.shift(1) < 0)),
            ((ema_diff < 0) & (ema_diff.shift(1) > 0))
        ]
        choices = ["Long", "Short"]
        self.df["EMA_bullbear"] = np.select(conditions_1, choices, default=None)

        self.df[f"EMA{self.ema_short}_slope"] = self.df[f"EMA{self.ema_short}"].diff()
        conditions_2 = [
            (self.df[f"EMA{self.ema_short}_slope"] > 0),
            (self.df[f"EMA{self.ema_short}_slope"] < 0)
        ]
        trend = ["Up", "Down"]
        self.df[f"EMA{self.ema_short}_trend"] = np.select(conditions_2, trend, default=None)

        return
    
    def _mark_price_peaks_troughs_detail(self):

        self.df = self.df.copy()

        prices = self.df[self.input_price_1].values
        peak_idx, _ = find_peaks(prices,distance=self.extrema_price_gap,prominence=self.extrema_price_prominence)
        trough_idx, _ = find_peaks(-prices,distance=self.extrema_price_gap,prominence=self.extrema_price_prominence)

        self.df["price_Peak"] = False
        self.df["price_Trough"] = False

        self.df.loc[peak_idx, "price_Peak"] = True
        self.df.loc[trough_idx, "price_Trough"] = True

        self.df["Higher_Peak"] = False
        self.df["Lower_Trough"] = False
        self.df["price_search"] = None

        # 找出「所有被標記為峰頂」的 index 位置
        peak_indices = self.df.index[self.df["price_Peak"] == True].tolist()

        for i in range(self.search_quantity, len(peak_indices)) :

            # 取 input_price_1 對應的 ATR 作為價格差容忍
            max_diff = self.df.loc[peak_indices[i], "ATR"]

            earliest_index = None
            for j in range(i - self.search_quantity , i):

                diff = abs(self.df.loc[peak_indices[i], self.input_price_1]
                           - self.df.loc[peak_indices[j], self.input_price_1])
                
                if ((diff <= max_diff) and
                    (self.df.loc[peak_indices[i], self.input_price_1] > self.df.loc[peak_indices[j], self.input_price_1])):

                    if earliest_index == None :
                        earliest_index = j
                        self.df.loc[peak_indices[i], "Higher_Peak"] = True

            if self.df.loc[peak_indices[i], "Higher_Peak"] != True:
                self.df.loc[peak_indices[i], "price_search"] = None
            else:
                self.df.loc[peak_indices[i], "price_search"] = peak_indices[earliest_index]

        # 找出「所有被標記為低點」的 index 位置
        trough_indices = self.df.index[self.df["price_Trough"] == True].tolist()

        for i in range(self.search_quantity, len(trough_indices)) :

            # 取 input_price_1 對應的 ATR 作為價格差容忍
            max_diff = self.df.loc[trough_indices[i], "ATR"]

            earliest_index = None
            for j in range(i - self.search_quantity , i):

                diff = abs(self.df.loc[trough_indices[i], self.input_price_1]
                           - self.df.loc[trough_indices[j], self.input_price_1])
                
                if ((diff <= max_diff) and
                    (self.df.loc[trough_indices[i], self.input_price_1] < self.df.loc[trough_indices[j], self.input_price_1])):

                    if earliest_index == None :
                        earliest_index = j
                        self.df.loc[trough_indices[i], "Lower_Trough"] = True

            if self.df.loc[trough_indices[i], "Lower_Trough"] != True:
                self.df.loc[trough_indices[i], "price_search"] = None
            else:
                self.df.loc[trough_indices[i], "price_search"] = trough_indices[earliest_index]

        return

    def _mark_FI_2_peaks_troughs_detail(self):

        self.df = self.df.copy()

        FI_values = self.df[self.indicator_pair_2[1]].values
        peak_idx, _ = find_peaks(FI_values,distance=self.extrema_FI_2_gap,prominence=self.extrema_FI_2_prominence)
        trough_idx, _ = find_peaks(-FI_values,distance=self.extrema_FI_2_gap,prominence=self.extrema_FI_2_prominence)

        # 過濾：peak 只保留正值，trough 只保留負值
        peak_idx = peak_idx[FI_values[peak_idx] > 0]
        trough_idx = trough_idx[FI_values[trough_idx] < 0]

        self.df[f"{self.indicator_pair_2[1]}_Peak"] = False
        self.df[f"{self.indicator_pair_2[1]}_Trough"] = False
        self.df.loc[peak_idx, f"{self.indicator_pair_2[1]}_Peak"] = True
        self.df.loc[trough_idx, f"{self.indicator_pair_2[1]}_Trough"] = True

        return

    def _mark_FI_13_peaks_troughs_detail(self):

        self.df = self.df.copy()

        self.df["FI_13_diff"] = self.df[self.indicator_pair_1[1]] - self.df[self.indicator_pair_1[1]].shift(1) 

        peak_cond = (
            (self.df["FI_13"] > 0) &
            (self.df["FI_13_diff"] > 0) &
            (abs(self.df["FI_13_diff"] / self.df[self.indicator_pair_1[1]].shift(1) ) * 100 >= self.pair_1_threshold[1])
        )

        trough_cond = (
            (self.df["FI_13"] < 0) &
            (self.df["FI_13_diff"] < 0) &
            (abs(self.df["FI_13_diff"] / self.df[self.indicator_pair_1[1]].shift(1) ) * 100 >= self.pair_1_threshold[1])
        )

        self.df[f"{self.indicator_pair_1[1]}_Peak"] = peak_cond
        self.df[f"{self.indicator_pair_1[1]}_Trough"] = trough_cond

        return

    def _mark_trend_1(self):
        
        self.df = self.df.copy()

        self.df[f"{self.indicator_pair_1[0]}_trend"] = None
        self.df[f"{self.indicator_pair_1[1]}_trend"] = None
        self.df[f"{self.indicator_pair_1[0]}_trend_pair"] = None
        self.df[f"{self.indicator_pair_1[1]}_trend_pair"] = None

        # 峰頂標記欄位名稱（字串），"Higher_Peak"=True 的列才參與 Down 判斷
        higher_peak_indices = self.df.index[self.df["Higher_Peak"] == True].tolist()

        for i in range(len(higher_peak_indices)) :
            search_index = self.df.loc[higher_peak_indices[i], "price_search"]

            for indicator, threshold in zip(self.indicator_pair_1, self.pair_1_threshold) :
                conditions = []

                for j in range(search_index, higher_peak_indices[i]) :
                
                    if self.df.loc[higher_peak_indices[i], indicator] < self.df.loc[j, indicator] :

                        condition = abs(self.df.loc[higher_peak_indices[i], indicator] - self.df.loc[j, indicator]) \
                                    /abs(self.df.loc[j, indicator]) *100
                        
                        if condition >= threshold :
                            conditions.append((j, condition))
                            
                if conditions :
                    top1 = heapq.nlargest(1, conditions, key=lambda x: x[1])
                    self.df.loc[higher_peak_indices[i], f"{indicator}_trend"] = "Down"
                    self.df.loc[higher_peak_indices[i], f"{indicator}_trend_pair"] = f"{top1[0][0]}_{higher_peak_indices[i]}"

        # 谷底標記欄位名稱（字串），"Lower_Peak" =True 的列才參與 Up 判斷
        lower_trough_indices = self.df.index[self.df["Lower_Trough"] == True].tolist()
        
        for i in range(len(lower_trough_indices)) :
            search_index = self.df.loc[lower_trough_indices[i], "price_search"]

            for indicator, threshold in zip(self.indicator_pair_1, self.pair_1_threshold) :
                conditions = []

                for j in range(search_index, lower_trough_indices[i]) :
                
                    if self.df.loc[lower_trough_indices[i], indicator] > self.df.loc[j, indicator] :

                        condition = abs(self.df.loc[lower_trough_indices[i], indicator] - self.df.loc[j, indicator]) \
                                    /abs(self.df.loc[j, indicator]) *100
                        
                        if condition >= threshold :
                            conditions.append((j, condition))
                            
                if conditions :
                    top1 = heapq.nlargest(1, conditions, key=lambda x: x[1])
                    self.df.loc[lower_trough_indices[i], f"{indicator}_trend"] = "Up"
                    self.df.loc[lower_trough_indices[i], f"{indicator}_trend_pair"] = f"{top1[0][0]}_{lower_trough_indices[i]}"

        return
    
    def _mark_trend_2(self):
        pass

    def _mark_trend_3(self):
        
        self.df = self.df.copy()

        self.df[f"{self.indicator_pair_3[0]}_trend"] = None
        self.df[f"{self.indicator_pair_3[1]}_trend"] = None
        self.df[f"{self.indicator_pair_3[0]}_trend_pair"] = None
        self.df[f"{self.indicator_pair_3[1]}_trend_pair"] = None

        # 峰頂標記欄位名稱（字串），"Higher_Peak"=True 的列才參與 Down 判斷
        higher_peak_indices = self.df.index[self.df["Higher_Peak"] == True].tolist()

        for i in range(len(higher_peak_indices)) :
            search_index = self.df.loc[higher_peak_indices[i], "price_search"]

            conditions = []
            for j in range(search_index, higher_peak_indices[i]) :

                if self.df.loc[j, self.indicator_pair_3[0]] > 0 :
                    conditions.append( (j, abs(self.df.loc[j, self.indicator_pair_3[0]])) )
            
            if conditions :        
                top2 = heapq.nlargest(2, conditions, key=lambda x: x[1])

                if len(top2) >= 2 and top2[0][0] < top2[1][0] :

                    self.df.loc[higher_peak_indices[i], f"{self.indicator_pair_3[0]}_trend"] = "Down"
                    self.df.loc[higher_peak_indices[i], f"{self.indicator_pair_3[0]}_trend_pair"] = f"{top2[0][0]}_{top2[1][0]}"

        # 谷底標記欄位名稱（字串），"Lower_Peak" =True 的列才參與 Up 判斷
        lower_trough_indices = self.df.index[self.df["Lower_Trough"] == True].tolist()
        
        for i in range(len(lower_trough_indices)) :
            search_index = self.df.loc[lower_trough_indices[i], "price_search"]

            conditions = []
            for j in range(search_index, lower_trough_indices[i]) :

                if self.df.loc[j, self.indicator_pair_3[1]] < 0 :
                    conditions.append( (j, abs(self.df.loc[j, self.indicator_pair_3[1]])) )
            
            if conditions :                 
                top2 = heapq.nlargest(2, conditions, key=lambda x: x[1])

                if len(top2) >= 2 and top2[0][0] < top2[1][0] :

                    self.df.loc[lower_trough_indices[i], f"{self.indicator_pair_3[1]}_trend"] = "Up"
                    self.df.loc[lower_trough_indices[i], f"{self.indicator_pair_3[1]}_trend_pair"] = f"{top2[0][0]}_{top2[1][0]}"

        return

    def _mark_Long_Short_1(self):
        """
        根據 indicator_pair_1 對應的 _trend 欄位標記 Long / Short
        
        標記邏輯：
        - trend_cols 所有欄位同時為 "Up"   -> side_pair_1 = "Long"
        - trend_cols 所有欄位同時為 "Down" -> side_pair_1 = "Short"
        - 其餘                             -> side_pair_1 = None
        """
        self.df = self.df.copy()
        
        # 先建立欄位，全部填 None
        self.df[self.side_pair_1] = None

        # 根據 indicator_pair_1 取得對應的 _trend 欄位名稱
        trend_cols = [indicator_col + "_trend" for indicator_col in self.indicator_pair_1]

        # 條件：trend_cols 所有欄位同時為 "Up"
        Long_cond  = self.df[trend_cols].eq("Up").all(axis=1)
        
        # 條件：trend_cols 所有欄位同時為 "Down"
        Short_cond = self.df[trend_cols].eq("Down").all(axis=1)
        
        # 標記 Long / Short
        self.df.loc[Long_cond,  self.side_pair_1] = "Long"
        self.df.loc[Short_cond, self.side_pair_1] = "Short"
        
        return

    def _mark_Long_Short_2(self):

        self.df = self.df.copy()
        # 根據 indicator_pair_2 取得對應的 _trend 欄位名稱

        conditions = [
            (self.df[f"{self.indicator_pair_2[0]}_trend"] == "Up")   & (self.df[f"{self.indicator_pair_2[1]}_Trough"] == True),
            (self.df[f"{self.indicator_pair_2[0]}_trend"] == "Down") & (self.df[f"{self.indicator_pair_2[1]}_Peak"]   == True)
        ]
        choices = ["Long", "Short"]

        self.df[self.side_pair_2] = np.select(conditions, choices, default=None)

        return

    def _mark_Long_Short_3(self):

        self.df = self.df.copy()

        # 根據 indicator_pair_3 取得對應的 _trend 欄位名稱
        bull_trend_col = f"{self.indicator_pair_3[0]}_trend"  # "Bull_Power_trend"
        bear_trend_col = f"{self.indicator_pair_3[1]}_trend"  # "Bear_Power_trend"

        conditions = [
            (self.df[bear_trend_col] == "Up") & (self.df["Lower_Trough"] == True),
            (self.df[bull_trend_col] == "Down") & (self.df["Higher_Peak"] == True)
        ]
        choices = ["Long", "Short"]

        self.df[self.side_pair_3] = np.select(conditions, choices, default=None)

        return

    def mark_all(self):
        steps = [
            self._mark_EMA_bullbear,
            self._mark_price_peaks_troughs_detail,
            self._mark_FI_2_peaks_troughs_detail,
            self._mark_FI_13_peaks_troughs_detail,
            self._mark_trend_1,
            self._mark_trend_3,
            self._mark_Long_Short_1,
            self._mark_Long_Short_2,
            self._mark_Long_Short_3
        ]
        for step in steps:
            step()
        return self.df


