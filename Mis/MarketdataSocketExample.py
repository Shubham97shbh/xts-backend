import json
import uuid
from sqlalchemy import create_engine, text
from Connect import XTSConnect
from MarketDataSocketClient import MDSocket_io
from dotenv import load_dotenv
import os
import datetime
from app import process_data
import pandas as pd

connection_string = "mysql+mysqldb://root:root@127.0.0.1:3306/zerodha"
engine = create_engine(connection_string, echo=False)
conn = engine.connect()
load_dotenv()

data = []

import os

def update_env_file(file_path, key, value):
    # Read the contents of the existing .env file
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Update the contents
    updated_lines = []
    for line in lines:
        if line.startswith(key + '='):
            # If the line starts with the key, update the value
            line = f'{key}="{value}"\n'
        updated_lines.append(line)

    # Write the updated contents back to the .env file
    with open(file_path, 'w') as file:
        file.writelines(updated_lines)


def fetch_token(data, segment, atm_strike):
    start, end = 0, 0
    if segment == "NIFTY":
        start = atm_strike - (15 * 50)
        end = atm_strike + (15 * 50)
    if segment == "BANKNIFTY":
        start = atm_strike - (15 * 100)
        end = atm_strike + (15 * 100)
    if segment == "MIDCPNIFTY":
        start = atm_strike - (15 * 25)
        end = atm_strike + (15 * 25)
    if segment == "FINNIFTY":
        start = atm_strike - (15 * 50)
        end = atm_strike + (15 * 50)
    if segment == "BANKEX":
        start = atm_strike - (15 * 100)
        end = atm_strike + (15 * 100)
    if segment == "SENSEX":
        start = atm_strike - (15 * 100)
        end = atm_strike + (15 * 100)
    query = text(
        f"""
        SELECT i.* FROM instruments i JOIN (
        SELECT DISTINCT ContractExpiration FROM instruments WHERE Name='{segment}'
        ORDER BY ContractExpiration LIMIT 1
        )subq ON i.ContractExpiration = subq.ContractExpiration
        WHERE i.Name = '{segment}'
        AND i.StrikePrice BETWEEN {start} AND {end}
        ORDER BY i.StrikePrice;
        """
    )
    result = conn.execute(query)
    rows = result.fetchall()
    for row in rows:
        if row[0] == "NSEFO":
            data.append(
                {
                    "exchangeSegment": 2,
                    "exchangeInstrumentID": int(row[1]),
                    # "strikePrice": row[4],
                }
            )
        elif row[0] == "BSEFO":
            data.append(
                {
                    "exchangeSegment": 12,
                    "exchangeInstrumentID": int(row[1]),
                    # "strikePrice": row[4],
                }
            )
    return data


def get_atm(segment, id):
    atm = 0
    with engine.connect() as conn:
        atm = conn.execute(text(f"SELECT ltp from ind where ID='{id}'")).scalar()
    if atm is not None:
        if segment == "NIFTY":
            return int(round(atm / 50)) * 50
        if segment == "BANKNIFTY":
            return int(round(atm / 100)) * 100
        if segment == "SENSEX":
            return int(round(atm / 10)) * 10
        if segment == "MIDCPNIFTY":
            return int(round(atm / 75)) * 75
        if segment == "FINNIFTY":
            return int(round(atm / 40)) * 40
        if segment == "BANKEX":
            return int(round(atm / 15)) * 15
    else:
        print(f"Error in Getting LTP for {segment} with id: {id}")

# MarketData API Credentials
API_KEY = os.getenv("MARKET_KEY")
API_SECRET = os.getenv("MARKET_SECRET")
TOKEN = os.getenv("TOKEN")
source = "WEBAPI"

# Initialise
xt = XTSConnect(API_KEY, API_SECRET, source)

# Login for authorization token
response = xt.marketdata_login()
print(response)

#INTERACTIVE_KEY="INTERACTIVE_KEY="ad1dff3fe37873f609b595"
# Store the token and userid
# set_marketDataToken = TOKEN
# set_muserID = "JPKS4"
set_marketDataToken = response["result"]["token"]
set_muserID = response["result"]["userID"]
# print("Login: ", response)

# Setting up new token
# Example usage
env_file_path = '/home/ubuntu/DRF_Django/XTS/strategies/.env'
key_to_update = 'TOKEN'
new_value = os.getenv("TOKEN")
update_env_file(env_file_path, key_to_update, new_value)

# Connecting to Marketdata socket
soc = MDSocket_io(set_marketDataToken, set_muserID)


# # Instruments for subscribing
symbols = {
    "NIFTY": 26000,
    "BANKNIFTY": 26001,
    "FINNIFTY": 26034,
    "MIDCPNIFTY": 26121,
    "SENSEX": 26065,
    "BANKEX": 26118,
}
# for s, id in symbols.items():
#     atm_strike = get_atm(s, id)
#     print(f"{s} | {atm_strike}")
#     fetch_token(data, s, atm_strike)
nf, bnf, sn, bn, mid, fin = [], [], [], [], [], []
nf = fetch_token(nf, "NIFTY", get_atm("NIFTY", 26000))
bnf = fetch_token(bnf, "BANKNIFTY", get_atm("BANKNIFTY", 26001))
mid = fetch_token(mid, "FINNIFTY", get_atm("FINNIFTY", 26034))
fin = fetch_token(fin, "MIDCPNIFTY", get_atm("MIDCPNIFTY", 26121))
sn = fetch_token(sn, "SENSEX", get_atm("SENSEX", 26065))
bn = fetch_token(bn, "BANKEX", get_atm("BANKEX", 26118))

print(len(nf))
print(len(bnf))
print(len(mid))
print(len(fin))
print(len(sn))
print(len(bn))

data.append({"exchangeSegment": 1, "exchangeInstrumentID": 26000})
data.append({"exchangeSegment": 1, "exchangeInstrumentID": 26001})
data.append({"exchangeSegment": 1, "exchangeInstrumentID": 26121})
data.append({"exchangeSegment": 1, "exchangeInstrumentID": 26034})
data.append({"exchangeSegment": 11, "exchangeInstrumentID": 26065})
data.append({"exchangeSegment": 11, "exchangeInstrumentID": 26118})

# Callback for connection
def on_connect():
    """Connect from the socket."""
    print("Market Data Socket connected successfully!")

    # # Subscribe to instruments
    print(f"Sending subscription request for Instruments - {len(data)+len(nf)+len(nf)+len(mid)+len(fin)+len(sn)+len(bn)}")
    temp = nf+bnf+data+mid+fin+sn+bn
    xt.send_subscription(data, 1501)
    xt.send_subscription(nf, 1501)
    xt.send_subscription(bnf, 1501)
    xt.send_subscription(sn, 1501)
    response = xt.send_subscription(bn, 1501)
    print("Sent Subscription request!")
    print("Subscription response: ", response)


def on_disconnect():
    print("Market Data Socket disconnected!")


def on_error(data):
    """Error from the socket."""
    print("Market Data Error", data)

import requests
class Service:
    def __init__(self):
        self.url = 'http://0.0.0.0:8001'
    def get_data(self, idx):
        URL = self.url+'/get_data'
        body = {'id':idx}
        data = requests.get(url=URL, json=body)
        return data.json().get("ltp")
    def update_data(self, id, ltp):
        URL = self.url+'/update_data'
        body = {'id':id, 'ltp':ltp}
        response = requests.put(url=URL, json=body)
        print(response.json)

service = Service()

def on_message1512_json_full(data):
    try:
        data = json.loads(data)
        if "LastTradedPrice" in data:
            ltp = data["LastTradedPrice"]
            o = data["Open"]
            h = data["High"]
            l = data["Low"]
            c = data["Close"]
            ask = data["AskInfo"]["Price"]
            bid = data["BidInfo"]["Price"]
            #process_data.delay(ltp, data["ExchangeInstrumentID"])
            #update_rates(data["ExchangeInstrumentID"], ltp, ns)
            service.update_data(data["ExchangeInstrumentID"], ltp)
        if "Touchline" in data:
            d = data["Touchline"]
            ltp = d["LastTradedPrice"]
            o = d["Open"]
            h = d["High"]
            l = d["Low"]
            c = d["Close"]
            ask = d["AskInfo"]["Price"]
            bid = d["BidInfo"]["Price"]
            #update_rates(data["ExchangeInstrumentID"], ltp, ns)
            service.update_data(data["ExchangeInstrumentID"], ltp)
            # print(ns.RATES.head())
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()


soc.on_connect = on_connect
soc.on_disconnect = on_disconnect
soc.on_error = on_error


# Event listener
el = soc.get_emitter()
el.on("connect", on_connect)
el.on("1501-json-full", on_message1512_json_full)

soc.connect()
