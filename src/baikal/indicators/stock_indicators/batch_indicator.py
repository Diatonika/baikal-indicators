import datetime
import logging

from collections.abc import Generator, Iterable
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, cast, overload

from pandera.typing.polars import DataFrame
from polars import DataFrame as PolarDataFrame, col, concat
from rich.progress import Progress

from baikal.common.rich import with_handler
from baikal.common.rich.progress import DateTimeColumn, TimeFraction
from baikal.common.trade.models import OHLCV, TimeSeries
from baikal.common.trade.parquet import (
    ParquetTimeSeriesPartition,
    ParquetTimeSeriesWriter,
)
from baikal.indicators.stock_indicators._window_validator import validate_window
from baikal.indicators.stock_indicators.indicator import FieldMetadata, Indicator
from baikal.indicators.stock_indicators.transform import to_quotes


class BatchIndicator:
    LOGGER = logging.getLogger(__name__)

    def __init__(self, indicators: Iterable[Indicator[Any, Any]]) -> None:
        self._indicators = tuple(indicators)

    def metadata(self) -> dict[str, FieldMetadata]:
        metadata: dict[str, FieldMetadata] = {}
        for indicator in self._indicators:
            metadata.update(indicator.metadata())

        return metadata

    @overload
    def calculate(
        self,
        ohlcv: DataFrame[OHLCV],
        interval: datetime.timedelta | str,
        warmup_period: datetime.timedelta,
        window_size: datetime.timedelta,
        *,
        parquet_path: Path | None = None,
        return_frame: Literal[True] = True,
    ) -> PolarDataFrame: ...

    @overload
    def calculate(
        self,
        ohlcv: DataFrame[OHLCV],
        interval: datetime.timedelta | str,
        warmup_period: datetime.timedelta,
        window_size: datetime.timedelta,
        *,
        parquet_path: Path | None = None,
        return_frame: Literal[False] = False,
    ) -> None: ...

    def calculate(
        self,
        ohlcv: DataFrame[OHLCV],
        interval: datetime.timedelta | str,
        warmup_period: datetime.timedelta,
        window_size: datetime.timedelta,
        *,
        parquet_path: Path | None = None,
        return_frame: bool = True,
    ) -> PolarDataFrame | None:
        if parquet_path is None and return_frame is False:
            error = "`parquet_path` or `return_frame` must be set."
            raise ValueError(error)

        progress = Progress(*Progress.get_default_columns(), DateTimeColumn("%Y-%m-%d"))
        with progress, self._with_writer(parquet_path) as writer:
            return self._calculate(
                ohlcv,
                interval,
                warmup_period,
                window_size,
                progress=progress,
                writer=writer,
                return_frame=return_frame,
            )

    @with_handler(LOGGER)
    def _calculate(
        self,
        ohlcv: DataFrame[OHLCV],
        interval: datetime.timedelta | str,
        warmup_period: datetime.timedelta,
        window_size: datetime.timedelta,
        *,
        progress: Progress,
        writer: ParquetTimeSeriesWriter[TimeSeries] | None,
        return_frame: bool,
    ) -> PolarDataFrame | None:
        aggregated_chunks: list[PolarDataFrame] = []

        min_date_time = cast(datetime.datetime, ohlcv["date_time"].min())
        max_date_time = cast(datetime.datetime, ohlcv["date_time"].max())
        left_border = min_date_time

        tracker = TimeFraction(min_date_time, max_date_time)
        task = progress.add_task(
            f"Indicators on [{min_date_time}, {max_date_time}]",
            current=min_date_time,
            target=max_date_time,
        )

        nullable_ohlcv = ohlcv.upsample("date_time", every=interval)
        while left_border <= max_date_time:
            progress.update(
                task,
                completed=tracker.fraction(left_border) * 100,
                current=left_border,
            )

            warmup_border = left_border + warmup_period
            window_border = left_border + window_size

            nullable_ohlcv = nullable_ohlcv.filter(col("date_time") >= left_border)
            nullable_chunk = nullable_ohlcv.filter(col("date_time") < window_border)

            window_parameters = validate_window(
                window_nullable_ohlcv=nullable_chunk,
                start=left_border,
                end=window_border,
                warmup=warmup_period,
            )

            if window_parameters.valid_end <= warmup_border:
                left_border = window_parameters.next_warmup
                continue

            chunk = DataFrame[OHLCV](
                nullable_chunk.filter(col("date_time") < window_parameters.valid_end)
            )

            quotes = to_quotes(chunk)
            indicators_chunk = [
                indicator.calculate(quotes) for indicator in self._indicators
            ]

            chunks = [chunk] + indicators_chunk
            aggregated_chunk = concat(chunks, how="align_left").filter(
                col("date_time") >= warmup_border,
            )

            if aggregated_chunk.null_count().sum_horizontal().item():
                self.LOGGER.warning(
                    f"Indicator chunk contains null values. "
                    f"Hint: increase warmup period.\n"
                    f"{aggregated_chunk.filter(col('*').is_null().any())}"
                )

            if writer is not None:
                writer.write(TimeSeries.validate(aggregated_chunk))

            if return_frame:
                aggregated_chunks.append(aggregated_chunk)

            left_border = window_parameters.next_warmup

        progress.update(task, completed=100)
        return concat(aggregated_chunks) if return_frame else None

    @contextmanager
    def _with_writer(
        self, parquet_path: Path | None
    ) -> Generator[ParquetTimeSeriesWriter[TimeSeries] | None, None, None]:
        if parquet_path is None:
            yield None
            return None

        writer = ParquetTimeSeriesWriter[TimeSeries](
            parquet_path, ParquetTimeSeriesPartition.MONTH
        )

        with writer:
            yield writer

        return None
