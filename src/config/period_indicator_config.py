# 全域預設的 INDICATOR 與 explain 參數

DEFAULT_INDICATOR_PARAMS = {
    "ADX": {"timeperiod":14},
    "MACD": {"fastperiod":12, "slowperiod":26, "signalperiod":9},
    "DI": {"timeperiod":13},
    "STOCH": {"fastk_period":5, "slowk_period":3, "slowd_period":3, "slowd_matype":0},
    "WILLR": {"timeperiod":7},
    "BBANDS_27": {"timeperiod":13, "nbdevup":2.7, "nbdevdn":2.7, "matype":1},
    "BBANDS_17": {"timeperiod":13, "nbdevup":1.7, "nbdevdn":1.7, "matype":1},
    "EMA": {"ema_period":[13,26]},
    "SMA": {"sma_period":[50,200]},
    "ATR": {"timeperiod":14},
    "FI": {"ema_period":[2,13]},
    "Bull_Bear_Power": {"ema_period":13}
}

DEFAULT_EXPLAIN_PARAMS = {
    "EMA_short": 13,
    "EMA_long": 26,
    "input_price_1": "Close",
    "minima_price_gap": 5,
    "minima_FI_2_gap": 5,
    "minima_price_prominence": 1,
    "minima_FI_2_prominence": 5,
    "search_quantity": 2,
    "indicators": {
        "Side_1" : {
            "MACD_hist": {"threshold": 30 },
            "FI_13":     {"threshold": 70 },
        },
        "Side_2" : {
            "EMA13":     {"threshold": None },
            "FI_2":      {"threshold": None },
        },
        "Side_3" : {
            "Bull_Power":{"threshold": None },
            "Bear_Power":{"threshold": None },
        }
    }
}

# 7 種 period 配置
indicator_and_period_configs = {
    "D": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "W": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "2W": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "3W": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "ME": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "2ME": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS}
    },
    "3ME": {
        "indicator_params": {**DEFAULT_INDICATOR_PARAMS},
        "explain_params": {**DEFAULT_EXPLAIN_PARAMS},
    }
}
