import datetime as dt
import os

import dotenv


dotenv.load_dotenv(override=True)

GRAD_TOKEN = os.getenv("GRAD_TOKEN")
UNDERGRAD_TOKEN = os.getenv("UNDERGRAD_TOKEN")
BC_TOKEN = os.getenv("BC_TOKEN")
QUANT_TOKEN = os.getenv("QUANT_TOKEN")
QUANT_PAPER_TOKEN = os.getenv("QUANT_PAPER_TOKEN")

configs = [
    {
        "fund": "grad",
        "token": GRAD_TOKEN,
        "queries": {
            "nav": "993010",
            "delta_nav": "993013",
            "positions": "993015",
            "dividends": "993011",
            "trades": "993012",
        },
    },
    {
        "fund": "undergrad",
        "token": UNDERGRAD_TOKEN,
        "queries": {
            "nav": "989615",
            "delta_nav": "989561",
            "positions": "989564",
            "dividends": "989565",
            "trades": "989567",
        },
    },
    {
        "fund": "brigham_capital",
        "token": BC_TOKEN,
        "queries": {
            "nav": "989606",
            "delta_nav": "989601",
            "positions": "989602",
            "dividends": "989603",
            "trades": "989605",
        },
    },
    {
        "fund": "quant",
        "token": QUANT_TOKEN,
        "queries": {
            "nav": "1029491",
            "delta_nav": "1029495",
            "positions": "1029501",
            "dividends": "1029496",
            "trades": "1029498",
        },
    },
    {
        "fund": "quant_paper",
        "token": QUANT_PAPER_TOKEN,
        "queries": {
            "nav": "1318734",
            "delta_nav": "1318735",
            "positions": "1318736",
            "dividends": "1318737",
            "trades": "1318746",
        },
    },
]

min_date = dt.date(2020, 1, 1)
