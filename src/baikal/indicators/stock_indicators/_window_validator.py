from datetime import datetime, timedelta
from typing import cast

from attrs import define
from pandera.typing.polars import DataFrame
from polars import Expr, col

from baikal.common.trade.models import OHLCV


@define
class WindowParameters:
    valid_end: datetime
    next_warmup: datetime


def validate_window(
    window_ohlcv: DataFrame[OHLCV],
    start: datetime,
    end: datetime,
    warmup: timedelta,
) -> WindowParameters:
    assert window_ohlcv["date_time"].len()

    segmented = window_ohlcv.with_columns(valid=_is_ohlcv_valid())
    segmented = segmented.with_columns(segment_id=col("valid").rle_id())

    first_segment = segmented.filter(segment_id=0)
    second_segment = segmented.filter(segment_id=1)

    # Window starts with missing data
    if not first_segment["valid"].first():
        return WindowParameters(
            valid_end=start,
            next_warmup=cast(datetime, second_segment["date_time"].first() or end),
        )

    # Window is fully valid
    if not second_segment["date_time"].len():
        return WindowParameters(
            valid_end=end,
            next_warmup=end - warmup,
        )

    third_segment = segmented.filter(segment_id=2)

    return WindowParameters(
        valid_end=cast(datetime, second_segment["date_time"].first() or end),
        next_warmup=cast(datetime, third_segment["date_time"].first() or end),
    )


def _is_ohlcv_valid() -> Expr:
    return (
        col("open").is_not_null()
        & col("high").is_not_null()
        & col("low").is_not_null()
        & col("close").is_not_null()
        & col("volume").is_not_null()
    )
