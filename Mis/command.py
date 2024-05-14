from sqlalchemy import create_engine, text

connection_string = "mysql+mysqldb://root:root@127.0.0.1:3306/zerodha"
engine = create_engine(connection_string, echo=False)
conn = engine.connect()

data = []


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


# Execute the combined query


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
                    "strikePrice": row[4],
                }
            )
        elif row[0] == "BSEFO":
            data.append(
                {
                    "exchangeSegment": 12,
                    "exchangeInstrumentID": int(row[1]),
                    "strikePrice": row[4],
                }
            )


symbols = {
    "NIFTY": 26000,
    "BANKNIFTY": 26001,
    "FINNIFTY": 26034,
    "MIDCPNIFTY": 26121,
    "SENSEX": 26065,
    "BANKEX": 26118,
}

for s, id in symbols.items():
    atm_strike = get_atm(s, id)
    atm = print(f"{s} | {atm_strike}")
    fetch_token(data, s, atm_strike)

data.append({"exchangeSegment": 1, "exchangeInstrumentID": 26000})
data.append({"exchangeSegment": 1, "exchangeInstrumentID": 26001})
data.append({"exchangeSegment": 1, "exchangeInstrumentID": 26121})
data.append({"exchangeSegment": 1, "exchangeInstrumentID": 26034})
data.append({"exchangeSegment": 11, "exchangeInstrumentID": 26065})
data.append({"exchangeSegment": 11, "exchangeInstrumentID": 26118})
print(len(data))
