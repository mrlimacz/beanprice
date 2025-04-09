"""A source fetching prices for investement funds managed by Millennium TFI S.A.
"""
from decimal import Decimal

import requests
from zoneinfo import ZoneInfo
from pandas import DataFrame, to_datetime

from beanprice import source


class MillenniumTFIError(ValueError):
    "An error from the Millennium TFI."


def _parse_ticker(ticker):
    """Parse the internal fund ID and other details from the fund full name
    Args:
      ticker: A string, fund's full name (for list see https://www.bankmillennium.pl/klienci-indywidualni/inwestycje), with underscores (_) instead of spaces
    Returns:
      Dictionary with internal fund id, currency, and company id
    """
    url = "https://portal.api.bankmillennium.pl/bm/app/portal/views/public/funds//investmentFunds"
    response = requests.get(url)
    if response.status_code != requests.codes.ok:
        raise MillenniumTFIError(
            f"Invalid response ({response.status_code}): {response.text}"
        )
    all_funds = response.json().get("items")
    if all_funds:
        fund_details = next(
            (
                {
                    key: item[key]
                    for key in item.keys()
                    if key in ["fundId", "companyId", "currency"]
                }
                for item in response.json().get("items")
                if item.get("fundDesc") == ticker.replace("_", " ")
            ),
            None,
        )
    else:
        raise MillenniumTFIError("No fund details found")
    return fund_details


def _get_quote(ticker, date=None):
    """Fetch a price from Millennium's API."""
    fund_details = _parse_ticker(ticker)

    response = requests.get(
        url="https://portal.api.bankmillennium.pl/bm/app/portal/views/public/funds//investmentFundsRates",
        params=fund_details,
    )

    if response.status_code != requests.codes.ok:
        raise MillenniumTFIError(
            "Invalid response ({}): {}".format(response.status_code, response.text)
        )

    result = DataFrame(response.json())
    result.date = to_datetime(result.date, yearfirst=True, utc=True).dt.tz_convert(
        "Europe/Warsaw"
    )
    result = result.set_index("date")

    if date:
        date = date.replace(
            hour=0, minute=0, second=0, tzinfo=ZoneInfo("Europe/Warsaw")
        )
    else:
        date = result.index.max().date().isoformat()

    selected_point = result.loc[date:date, "sharePrice"]
    if not selected_point.empty:
        price = Decimal(str(selected_point.iloc[0]))
        time = selected_point.index[0]
        return source.SourcePrice(price, time, fund_details["currency"])
    else:
        return None


class Source(source.Source):
    def get_latest_price(self, ticker):
        return _get_quote(ticker)

    def get_historical_price(self, ticker, time):
        return _get_quote(ticker, time)
