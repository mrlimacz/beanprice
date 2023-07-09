"""A source calculating bond accrued interest based on commodity metadata
"""

from beanprice import source
from datetime import datetime, date, time
import re
from pandas.tseries.offsets import DateOffset
from pandas import Series, date_range, merge_asof
from beanprice import source
from dateutil.tz import tzlocal
from decimal import Decimal


class FixedIncomeError(ValueError):
    "An error from the Fixed Income calculator."


class Source(source.Source):
    def get_latest_price(self, ticker, metadata):
        return _get_quote(ticker, metadata)

    def get_historical_price(self, ticker, metadata, time):
        return _get_quote(ticker, metadata, time)


def _get_quote(ticker, metadata, quote_date=None):

    # Check if all necessary metadata provided
    required_keys = {
        "fixed_income_interest_rates",
        "fixed_income_coupon_rate",
        "fixed_income_period_count",
        "fixed_income_period_duration",
        "fixed_income_nominal_value",
        "fixed_income_capitalization",
        "commodity_date",
    }
    if required_keys > metadata.keys():
        raise FixedIncomeError(
            f"The following metadata not provided: {', '. join(required_keys - metadata.keys())}"
        )

    # Parse start and end date
    start_date = metadata.get("commodity_date")
    if quote_date is None:
        end_date = datetime.today()
    else:
        end_date = quote_date
    start_date = datetime.combine(start_date, time(0, 0, 0), tzinfo=tzlocal())
    end_date = end_date.replace(hour=0, minute=0, second=0, tzinfo=tzlocal())

    # Parse period lenght and interes rates
    match = re.match(
        r"^(?P<number>\d+)((?P<years>Y)|(?P<months>M))$",
        metadata.get("fixed_income_period_duration"),
    )
    if match:
        if match.group("years"):
            day_offset_for_period = DateOffset(years=int(match.group("number")))
        elif match.group("months"):
            day_offset_for_period = DateOffset(months=int(match.group("number")))
        else:
            raise FixedIncomeError(
                f"Incorrect period duration format, expected r'^\d+[YM]$'"
            )
    else:
        raise FixedIncomeError(
            f"Incorrect period duration format, expected r'^\d+[YM]$'"
        )
    interest_mapping = {
        key + 1: float(value) / 100
        for key, value in enumerate(
            metadata.get("fixed_income_interest_rates").split("/")
        )
    }

    # Determine date ranges and details for periods
    period_bounds = date_range(
        start=start_date,
        periods=int(metadata.get("fixed_income_period_count")) + 1,
        freq=day_offset_for_period,
        name="PeriodStartDate",
    )
    period_bounds = Series(
        range(1, len(period_bounds) + 1), index=period_bounds, name="PeriodNumber"
    ).reset_index()
    period_bounds["PeriodEndDate"] = period_bounds.PeriodStartDate.shift(-1)
    period_bounds = period_bounds.dropna()
    period_bounds["PeriodLength"] = (
        period_bounds.PeriodEndDate - period_bounds.PeriodStartDate
    )
    period_bounds["InterestRate"] = period_bounds.PeriodNumber.map(interest_mapping)
    if metadata.get("fixed_income_capitalization"):
        period_bounds["CumulativeInterestRate"] = (
            (1 + period_bounds.InterestRate)
            .cumsum()
            .fillna(method="ffill")
            .shift(1, fill_value=1)
        )
    else:
        period_bounds["CumulativeInterestRate"] = 1

    # Prepare daily date frame
    timeseries = date_range(start=start_date, freq="D", end=end_date, name="Date")
    timeseries = Series(timeseries).to_frame()
    timeseries = merge_asof(
        timeseries,
        period_bounds,
        left_on="Date",
        right_on="PeriodEndDate",
        allow_exact_matches=True,
        direction="forward",
    ).dropna()
    timeseries["DayDiff"] = timeseries.Date - timeseries.PeriodStartDate
    timeseries = timeseries.set_index("Date")

    # Calculate daily value, get the correct date
    timeseries["Price"] = (
        float(metadata.get("fixed_income_nominal_value"))
        * timeseries.CumulativeInterestRate
        * (
            1
            + timeseries.InterestRate
            * timeseries.DayDiff
            / timeseries.PeriodLength
            / int(metadata.get("fixed_income_coupon_rate"))
        )
    ).round(2)

    return source.SourcePrice(
        Decimal(timeseries.at[end_date, "Price"]), end_date, "PLN"
    )
