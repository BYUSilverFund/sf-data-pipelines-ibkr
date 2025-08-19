import datetime as dt

configs = [
    {
        "fund": "grad",
        "token": "547414482431868624428004",
        "queries": {
            "nav": "993010",
            "delta_nav": "993013",
            "positions": "993015",
            "dividends": "993011",
            "trades": "993012",
        }
    },
    {
        "fund": "undergrad",
        "token": "184072668161023104096150",
        "queries": {
            "nav": "989615",
            "delta_nav": "989561",
            "positions": "989564",
            "dividends": "989565",
            "trades": "989567",
        }
    },
    {
        "fund": "brigham_capital",
        "token": "1405366799704294639032",
        "queries": {
            "nav": "989606",
            "delta_nav": "989601",
            "positions": "989602",
            "dividends": "989603",
            "trades": "989605",
        }
    },
    {
        "fund": "quant",
        "token": "325702086386108763158099",
        "queries": {
            "nav": "1029491",
            "delta_nav": "1029495",
            "positions": "1029501",
            "dividends": "1029496",
            "trades": "1029498",
        }
    },
]

min_date = dt.date(2020, 1, 1)