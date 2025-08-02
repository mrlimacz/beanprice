"""A source fetching prices from analizy.pl for PPK funds"""

from decimal import Decimal

from dateutil.tz import tz
from dateutil.parser import parse
import requests

from beanprice import source


class AnalizyPlPPKError(ValueError):
    "An error from the analizy.pl PPK"


def _get_quote(ticker, date=None):
    """Fetch a PPK fund price from analizy.pl"""
    base_url = "https://www.analizy.pl/api/quotation/ppk"
    url = f"{base_url}/{ticker}"
    params = {"start": date.strftime("%Y-%m-%d"), "end": date.strftime("%Y-%m-%d")}

    response = requests.get(
        url=url,
        params=params,
    )

    if response.status_code != requests.codes.ok:
        raise AnalizyPlPPKError(
            "Invalid response ({}): {}".format(response.status_code, response.text)
        )

    response_json = response.json()
    try:
        assert (
            response_json.get("id") == ticker
        ), f"Requested ticker {ticker} does not match response {response_json.get("id")}"
    except AssertionError as e:
        raise AnalizyPlPPKError(e)

    try:
        currency = response_json["currency"]
        series = response_json["series"]
        selected_series = next((d for d in series if d.get("label") == "Fundusz"), {})
        prices = selected_series["price"]
        price = prices[-1]
        price_date = price["date"]
        price_value = price["value"]
    except KeyError as e:
        raise AnalizyPlPPKError(f"No elements {e} found in response")

    price_date = parse(price_date).replace(tzinfo=date.tzinfo)
    price_value = Decimal(price_value)

    return source.SourcePrice(price_value, price_date, currency)


class Source(source.Source):
    def get_latest_price(self, ticker):
        return _get_quote(ticker)

    def get_historical_price(self, ticker, time):
        return _get_quote(ticker, time)
