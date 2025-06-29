from collections.abc import Sequence

from pandera.typing.polars import DataFrame
from stock_indicators import Quote

from baikal.common.trade.models import OHLCV


def to_quotes(ohlcv: DataFrame[OHLCV]) -> Sequence[Quote]:
    """Transform OHLCV DataFrame to .NET Quote Sequence.

    Parameters
    ----------
    ohlcv: DataFrame[OHLCV]
        Continuous OHLCV DataFrame.
        No time gaps are allowed, but trade values may be null.

    Returns
    -------
    Sequence[Quote]
        .NET-bound Quote Sequence.

    Warnings
    --------
    .NET binding ignores time-zone information,
    so time-zone info is lost on .NET conversion.

    Avoid using this method on large OHLCV sequences,
    as .NET-binding is very expensive.
    """
    native_types = ohlcv.to_dicts()

    return [
        Quote(
            date=bar["date_time"].replace(tzinfo=None),
            open=bar["open"],
            high=bar["high"],
            low=bar["low"],
            close=bar["close"],
            volume=bar["volume"],
        )
        for bar in native_types
    ]
