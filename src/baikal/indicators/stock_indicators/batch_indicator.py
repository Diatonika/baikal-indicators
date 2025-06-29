import datetime

from collections.abc import Generator, Iterable
from contextlib import contextmanager
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Literal, cast, overload

from pandera.polars import DataFrameModel
from pandera.typing.polars import DataFrame
from polars import DataFrame as PolarDataFrame, col, concat
from rich.progress import Progress

from baikal.common.rich.progress import DateTimeColumn, TimeFraction
from baikal.common.trade.models import OHLCV
from baikal.indicators.stock_indicators.transform import to_quotes
from baikal.indicators.stock_indicators.indicator import Indicator


class BatchIndicator:
    def __init__(self, indicators: Iterable[Indicator[Any, Any]]) -> None:
        self._indicators = tuple(indicators)

    @overload
    def calculate(
        self,
        ohlcv: DataFrame[OHLCV],
        warmup_period: datetime.timedelta,
        window_size: datetime.timedelta,
        *,
        save_csv: Path | None = None,
        return_frame: Literal[False],
    ) -> None: ...

    @overload
    def calculate(
        self,
        ohlcv: DataFrame[OHLCV],
        warmup_period: datetime.timedelta,
        window_size: datetime.timedelta,
        *,
        save_csv: Path | None = None,
        return_frame: Literal[True],
    ) -> DataFrame[DataFrameModel]: ...

    def calculate(
        self,
        ohlcv: DataFrame[OHLCV],
        warmup_period: datetime.timedelta,
        window_size: datetime.timedelta,
        *,
        save_csv: Path | None = None,
        return_frame: bool = True,
    ) -> DataFrame[DataFrameModel] | None:
        if save_csv is None and return_frame is False:
            error = "`save_csv` or `return_frame` must be set."
            raise ValueError(error)

        progress = Progress(*Progress.get_default_columns(), DateTimeColumn("%Y-%m-%d"))
        with progress, self._with_file(save_csv) as file_descriptor:
            return self._calculate(
                ohlcv,
                warmup_period,
                window_size,
                progress=progress,
                file_descriptor=file_descriptor,
                return_frame=return_frame,
            )

    def _calculate(
        self,
        ohlcv: DataFrame[OHLCV],
        warmup_period: datetime.timedelta,
        window_size: datetime.timedelta,
        *,
        progress: Progress,
        file_descriptor: TextIOWrapper | None,
        return_frame: bool,
    ) -> DataFrame[DataFrameModel] | None:
        aggregated_chunks: list[PolarDataFrame] = []

        min_date_time = cast(datetime.datetime, ohlcv["date_time"].min())
        max_date_time = cast(datetime.datetime, ohlcv["date_time"].max())
        current_date_time = min_date_time

        time_fraction = TimeFraction(min_date_time, max_date_time)
        task = progress.add_task(
            f"Indicators on [{min_date_time}, {max_date_time})",
            current=min_date_time,
            target=max_date_time,
        )

        while current_date_time < max_date_time:
            progress.update(
                task,
                completed=time_fraction.fraction(current_date_time) * 100,
                current=current_date_time,
            )

            warmup_border = current_date_time + warmup_period
            window_border = warmup_border + window_size

            raw_chunk = ohlcv.filter(
                col("date_time") >= current_date_time,
                col("date_time") < window_border,
            )

            chunk = DataFrame[OHLCV](raw_chunk)
            quotes = to_quotes(chunk)
            indicators_chunk = [
                indicator.calculate(quotes) for indicator in self._indicators
            ]

            chunks = [chunk] + indicators_chunk
            aggregated_chunk = concat(chunks, how="align_left")
            if current_date_time != min_date_time:
                aggregated_chunk = aggregated_chunk.filter(
                    col("date_time") >= warmup_border,
                )

            if file_descriptor is not None:
                aggregated_chunk.write_csv(
                    file_descriptor,
                    include_header=current_date_time == min_date_time,
                )

            if return_frame is not None:
                aggregated_chunks.append(aggregated_chunk)

            current_date_time += window_size

        return None if return_frame is None else DataFrame(concat(aggregated_chunks))

    @contextmanager
    def _with_file(
        self, save_path: Path | None
    ) -> Generator[TextIOWrapper | None, None, None]:
        if save_path is None:
            yield None
            return None

        if not save_path.exists():
            save_path.touch()

        file_descriptor = save_path.open("w")

        try:
            yield file_descriptor
        finally:
            file_descriptor.close()
