import datetime
from dotenv import load_dotenv
from datetime import datetime, timedelta
import requests
from utils.config import URL_PATH

load_dotenv()


class Service:

    def __init__(self):
        self.url = URL_PATH

    def get_data(self, idx):
        URL = self.url + "/get_data"
        body = {"id": idx}
        data = requests.get(url=URL, json=body)
        return data.json().get("ltp")

    def update_data(self, id, ltp):
        URL = self.url + "/update_data"
        body = {"id": id, "ltp": ltp}
        response = requests.put(url=URL, json=body)
        print(response.json)

service = Service()

class NiftyTrade:
    
    def __init(self, xt):
            self.source = "WEBAPI"
            self.xt = None
    
    def get_exchange_instrumentid(self, index, side, strike, expiry):
        exchange_segment = 2
        _series = "OPTIDX"
        if index in ["BKX", "BSX", "SENSEX", "BANKEX"]:
            exchange_segment = 12
            _series = "IO"
        print(f"CONFIG: {exchange_segment,_series, index, side, strike, expiry}")
        response = self.xt.get_option_symbol(
            exchangeSegment=exchange_segment,
            series=_series,
            symbol=index,
            optionType=side,
            strikePrice=strike,
            expiryDate=expiry,
        )
        print(exchange_segment, _series, index, side, strike, expiry)
        print(f"Get Exchange Instrument ID: {response}")
        data = response["result"][0]
        exchange_instrument_id = data["ExchangeInstrumentID"]
        instrument = data["Description"]
        return exchange_instrument_id, instrument

    def next_thursday(self):
        date = datetime.today()	
        days_until_thursday = (3 - date.weekday()) % 7  # Calculate days until next Thursday (Thursday is represented by 3 in Python)
        date = date + timedelta(days=days_until_thursday)
        date = date.strftime("%d%b%Y")
        return date
    
    def get_exchange_instrument_id(self, index, strike, side):
        EXPIRY_DATE = self.next_thursday()
        exchange_instrument_id, instrument = self.get_exchange_instrumenid(
                self.xt, index, side, strike, EXPIRY_DATE
            )
        return exchange_instrument_id

class Data:
    def __init__(self):
        self.exchange_instrument_id = ''
    
    @staticmethod
    def get_ltp(exchange_instrument_id:int):
        return service.get_data(exchange_instrument_id)
    @staticmethod
    def get_bid(self, bid):
        pass