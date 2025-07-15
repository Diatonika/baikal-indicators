from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import DataFrame as PolarDataFrame, Float64
from pydantic import BaseModel
from stock_indicators import ChandelierType, Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import FieldMetadata, Indicator, RangeType


class ChandelierExitConfig(BaseModel):
    lookback_periods: int = 22
    multiplier: float = 3.0


class ChandelierExitModel(TimeSeries):
    chandelier_long: Float64  # ABSOLUTE
    chandelier_short: Float64  # ABSOLUTE


class ChandelierExit(Indicator[ChandelierExitConfig, ChandelierExitModel]):
    @classmethod
    def model(cls) -> type[ChandelierExitModel]:
        return ChandelierExitModel

    @classmethod
    def metadata(cls) -> dict[str, FieldMetadata]:
        return {
            "chandelier_long": FieldMetadata(range_type=RangeType.ABSOLUTE),
            "chandelier_short": FieldMetadata(range_type=RangeType.ABSOLUTE),
        }

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[ChandelierExitModel]:
        long = self._calculate(quotes, ChandelierType.LONG).rename(
            {"value": "chandelier_long"}
        )

        short = self._calculate(quotes, ChandelierType.SHORT).rename(
            {"value": "chandelier_short"}
        )

        return DataFrame[ChandelierExitModel](
            long.join(short, on="date_time", how="left", maintain_order="left")
        )

    def _calculate(
        self, quotes: Iterable[Quote], version: ChandelierType
    ) -> PolarDataFrame:
        results = indicators.get_chandelier(
            quotes, **self._config.model_dump(), chandelier_type=version
        ).remove_warmup_periods()

        return DataFrame[ChandelierExitModel](
            {
                "date_time": [value.date for value in results],
                "value": [value.chandelier_exit for value in results],
            }
        )
