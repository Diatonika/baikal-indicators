from collections.abc import Iterable

from pandera.typing.polars import DataFrame
from polars import DataFrame as PolarDataFrame, Float64
from pydantic import BaseModel
from stock_indicators import Quote, indicators

from baikal.common.trade.models import TimeSeries
from baikal.indicators.stock_indicators import Indicator


class FractalChaosBandsConfig(BaseModel):
    window_span: int = 2


class FractalChaosBandsModel(TimeSeries):
    fractal_chaos_upper: Float64
    fractal_chaos_lower: Float64


class FractalChaosBands(Indicator[FractalChaosBandsConfig, FractalChaosBandsModel]):
    @classmethod
    def model(cls) -> type[FractalChaosBandsModel]:
        return FractalChaosBandsModel

    def calculate(self, quotes: Iterable[Quote]) -> DataFrame[FractalChaosBandsModel]:
        results = indicators.get_fcb(
            quotes, **self._config.model_dump()
        ).remove_warmup_periods()

        polar = PolarDataFrame(
            {
                "date_time": [value.date for value in results],
                "fractal_chaos_upper": [value.upper_band for value in results],
                "fractal_chaos_lower": [value.lower_band for value in results],
            }
        ).drop_nulls()

        return FractalChaosBandsModel.validate(polar)
