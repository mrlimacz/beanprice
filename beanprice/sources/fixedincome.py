"""A source calculating bond accrued interest based on commodity metadata"""

from datetime import datetime, time
import re
from decimal import Decimal
from pandas.tseries.offsets import DateOffset
from pandas import Series, date_range, merge_asof
from dateutil.tz import tzlocal
from beanprice import source


class FixedIncomeError(ValueError):
    "An error from the Fixed Income calculator."


class Source(source.Source):
    def __init__(self):
        self.requires_commodity_metadata = True

    def get_latest_price(self, ticker, commodity_metadata=None):
        return _get_quote(ticker, commodity_metadata=commodity_metadata)

    def get_historical_price(self, ticker, time, commodity_metadata=None):
        return _get_quote(
            ticker, quote_date=time, commodity_metadata=commodity_metadata
        )


def _get_quote(ticker, quote_date=None, commodity_metadata=None):

    # Check if metadata supplied
    try:
        assert isinstance(
            commodity_metadata, dict
        ), f"Commodity {ticker} does not have a metadata supplied"
    except AssertionError as exc:
        raise FixedIncomeError from exc

    # Check if all necessary metadata provided
    required_keys = {
        "fixed_income_interest_rates",
        "fixed_income_coupon_rate",
        "fixed_income_period_count",
        "fixed_income_period_duration",
        "fixed_income_nominal_value",
        "fixed_income_coupons_reinvested",
        "commodity_date",
    }
    if not required_keys.issubset(set(commodity_metadata.keys())):
        missing_keys = required_keys - commodity_metadata.keys()
        raise FixedIncomeError(
            f"The following metadata not provided: {', '. join(missing_keys)}"
        )

    # Parse start and end date
    start_date = commodity_metadata.get("commodity_date")
    if quote_date is None:
        end_date = datetime.today()
    else:
        end_date = quote_date
    start_date = datetime.combine(start_date, time(0, 0, 0), tzinfo=tzlocal())
    end_date = end_date.replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=tzlocal()
    )

    # Parse period lenght and interes rates
    match = re.match(
        r"^(?P<number>\d+)((?P<years>Y)|(?P<months>M))$",
        commodity_metadata.get("fixed_income_period_duration"),
    )
    if match:
        if match.group("years"):
            day_offset_for_period = DateOffset(years=int(match.group("number")))
        elif match.group("months"):
            day_offset_for_period = DateOffset(months=int(match.group("number")))
        else:
            raise FixedIncomeError(
                "Incorrect period duration format, expected r'^\\d+[YM]$'"
            )
    else:
        raise FixedIncomeError(
            "Incorrect period duration format, expected r'^\\d+[YM]$'"
        )
    interest_mapping = {
        key + 1: float(value) / 100
        for key, value in enumerate(
            commodity_metadata.get("fixed_income_interest_rates").split("/")
        )
    }

    # Determine date ranges and details for periods
    period_bounds = date_range(
        start=start_date,
        periods=int(commodity_metadata.get("fixed_income_period_count")) + 1,
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
    if commodity_metadata.get("fixed_income_coupons_reinvested") == "true":
        period_bounds["CumulativeInterestRate"] = (
            (1 + period_bounds.InterestRate)
            .cumprod()
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
        float(commodity_metadata.get("fixed_income_nominal_value"))
        * timeseries.CumulativeInterestRate
        * (
            1
            + timeseries.InterestRate
            * timeseries.DayDiff
            / timeseries.PeriodLength
            / int(commodity_metadata.get("fixed_income_coupon_rate"))
        )
    ).round(2)

    return source.SourcePrice(
        Decimal(timeseries.at[end_date, "Price"]), end_date, "PLN"
    )
