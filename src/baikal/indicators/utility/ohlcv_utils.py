from pandera.typing.polars import LazyFrame
from polars import col

from baikal.common.models import OHLCV


class OHLCVUtils:
    @staticmethod
    def remove_zero_volume(ohlcv: LazyFrame[OHLCV]) -> LazyFrame[OHLCV]:
        """Remove OHLCV values with zero-volume OHLCV bars

        Parameters
        ----------
        ohlcv: LazyFrame[OHLCV]
            OHLCV LazyFrame

        Returns
        -------
        LazyFrame[OHLCV]
            OHLCV without zero-volume OHLCV bars
        """
        return OHLCV.validate(ohlcv.filter(col(OHLCV.volume).ne(0)), lazy=True)
