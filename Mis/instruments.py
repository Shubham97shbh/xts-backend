from datetime import datetime
import json
from Connect import XTSConnect
import os
from dotenv import load_dotenv
import pandas as pd
import time
from sqlalchemy import create_engine, text

try:
    connection_string = "mysql+mysqldb://root:root@127.0.0.1:3306/zerodha"
    engine = create_engine(connection_string, echo=False)
    conn = engine.connect()
except Exception as e:
    print(f"error {e}")

load_dotenv()
API_KEY = os.getenv("DEMO_MARKET_KEY")
API_SECRET = os.getenv("DEMO_MARKET_SECRET")
source = "WEBAPI"

columns = [
    "ExchangeSegment",
    "ExchangeInstrumentID",
    "InstrumentType",
    "Name",
    "Description",
    "Series",
    "NameWithSeries",
    "InstrumentID",
    "PriceBand.High",
    "PriceBand.Low",
    "FreezeQty",
    "TickSize",
    "LotSize",
    "Multiplier",
    "UnderlyingInstrumentId",
    "UnderlyingIndexName",
    "ContractExpiration",
    "StrikePrice",
    "OptionType",
    "DisplayName",
    "PriceNumerator",
    "PriceDenominator",
    "DetailedDescription",
]
data_rows = []
xt = XTSConnect(API_KEY, API_SECRET, source)


def dump_variable_to_txt(variable, filename):
    with open(filename, "w") as file:
        file.write(str(variable))


# Login for authorization token
response = xt.marketdata_login()
print(response)
instruments = xt.get_master(exchangeSegmentList=["NSEFO", "BSEFO"])
dump_variable_to_txt(instruments["result"], "instrument.csv")
time.sleep(4)

with open("instrument.csv", "r") as file:
    for line_number, line in enumerate(file, start=1):
        try:
            # Split the line into values using the '|' separator
            values = line.strip().split("|")

            # Check if the number of values matches the expected number of columns
            if len(values) == len(columns):
                # Append the values as a dictionary to the data_rows list
                data_rows.append(dict(zip(columns, values)))
            else:
                pass
        except Exception as e:
            print(f"Error processing line {line_number}: {e}")
df = pd.DataFrame(data_rows)
print(len(df))
df = df[
    (df["UnderlyingIndexName"] == "Nifty Fin Service")
    | (df["UnderlyingIndexName"] == "Nifty Bank")
    | (df["UnderlyingIndexName"] == "Nifty 50")
    | (df["UnderlyingIndexName"] == "NIFTY MID SELECT")
    | (df["UnderlyingIndexName"] == "BANKEX")
    | (df["UnderlyingIndexName"] == "SENSEX")
]
df.to_sql("instruments", con=engine, index=False, if_exists="replace")

instruments = [
{"exchangeSegment": 1, "exchangeInstrumentID": 26000},
{"exchangeSegment": 1, "exchangeInstrumentID": 26001},
{"exchangeSegment": 1, "exchangeInstrumentID": 26121},
{"exchangeSegment": 1, "exchangeInstrumentID": 26034},
{"exchangeSegment": 11, "exchangeInstrumentID": 26065},
{"exchangeSegment": 11, "exchangeInstrumentID": 26118}
]
s = xt.get_quote(instruments, xtsMessageCode=1501, publishFormat="JSON")['result']
for d in s['listQuotes']:
    d = json.loads(d)
    id = int(d["ExchangeInstrumentID"])
    ltp = d['LastTradedPrice']
    conn.execute(text(f"INSERT INTO ind (Id, ltp) VALUES ({id}, {ltp}) ON DUPLICATE KEY UPDATE ltp={ltp} "))
    #conn.execute(text(f"INSERT INTO ind (Id, ltp) VALUES ({id}, {ltp})"))
    conn.commit()
    time.sleep(0.5)

def get_index_expiry_date(index):
    _exchangeSegment = 2
    _series = "OPTIDX"
    if index in ["BANKEX", "SENSEX"]:
        _series = "IO"
        _exchangeSegment = 12
    try:
        response = xt.get_expiry_date(
            exchangeSegment=_exchangeSegment, series=_series, symbol=index
        )
        dates = [datetime.fromisoformat(date) for date in response["result"]]
        sorted_dates = sorted(dates)
        return sorted_dates[0].strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        print(f"Error: {e}")
