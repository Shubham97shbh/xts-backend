import aiohttp

PRICES_URL = "http://beta-prices.ap-south-1.elasticbeanstalk.com"


def fetch_data(url):
    with aiohttp.ClientSession() as session:
        with session.get(url) as response:
            return response.json()


def getCurrentExpiryDetails():
    url = PRICES_URL + "/api/expiry"
    try:
        res = fetch_data(url)
        if res["status"] == "success":
            return res["data"]
        else:
            print("ERROR: ", res)
            return None
    except Exception as e:
        print("ERROR: ", e)
        return None
