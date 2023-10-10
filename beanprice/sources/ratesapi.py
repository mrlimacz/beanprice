"""A source fetching exchangerates from https://exchangerate.host.

Valid tickers are in the form "XXX-YYY", such as "EUR-CHF".

Here is the API documentation:
https://exchangerate.host/

For example:

https://api.exchangerate.host/api/latest?base=EUR&symbols=CHF


Timezone information: Input and output datetimes are specified via UTC
timestamps.
"""

from decimal import Decimal

import re
import requests
from dateutil.tz import tz
from dateutil.parser import parse

from beanprice import source


class RatesApiError(ValueError):
    "An error from the Rates API."

def _parse_ticker(ticker):
    """Parse the base and quote currencies from the ticker.

    Args:
      ticker: A string, the symbol in XXX-YYY format.
    Returns:
      A pair of (base, quote) currencies.
    """
    match = re.match(r'^(?P<base>\w+)-(?P<symbol>\w+)$', ticker)
    if not match:
        raise ValueError(
            'Invalid ticker. Use "BASE-SYMBOL" format.')
    return match.groups()

def _get_quote(ticker, date, metadata):
    """Fetch a exchangerate from ratesapi."""
    base, symbol = _parse_ticker(ticker)
    
    # Get API token (no longer possible to send requests without it)
    if metadata is None or metadata.get("ratesapi_token") is None:
        ratesapi_token = input("Provide API key (can be obtained for free, see https://exchangerate.host/): ")
    else:
        ratesapi_token = metadata.get("ratesapi_token")
   
    params = {
        'base': base,
        'symbol': symbol,
        'access_key': ratesapi_token
    }
    response = requests.get(url='http://api.exchangerate.host/' + date, params=params)

    if response.status_code != requests.codes.ok:
        raise RatesApiError("Invalid response ({}): {}".format(response.status_code,
                                                            response.text))

    result = response.json()

    price = Decimal(str(result['rates'][symbol]))
    time = parse(result['date']).replace(tzinfo=tz.tzutc())

    return source.SourcePrice(price, time, symbol)


class Source(source.Source):

    def get_latest_price(self, ticker, metadata):
        return _get_quote(ticker, 'latest', metadata)

    def get_historical_price(self, ticker, metadata, time):
        return _get_quote(ticker, time.date().isoformat(), metadata)
