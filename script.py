import datetime

from pathlib import Path

from baikal.common.trade.models import OHLCV
from baikal.converters.binance import (
    BinanceConverter,
    BinanceDataConfig,
    BinanceDataInterval,
    BinanceDataType,
    BinanceInstrumentType,
)
from baikal.indicators.stock_indicators import BatchIndicator
from baikal.indicators.stock_indicators.oscillator import *  # noqa: F403
from baikal.indicators.stock_indicators.price_channel import *  # noqa: F403
from baikal.indicators.stock_indicators.price_characteristic import *  # noqa: F403
from baikal.indicators.stock_indicators.price_trend import *  # noqa: F403
from baikal.indicators.stock_indicators.stop_reverse import *  # noqa: F403
from baikal.indicators.stock_indicators.volume import *  # noqa: F403
from baikal.indicators.utility import OHLCVUtils

if __name__ == "__main__":
    ohlcv = BinanceConverter(Path("./data")).load_ohlcv(
        BinanceDataConfig(
            BinanceDataType.OHLCV,
            BinanceInstrumentType.SPOT,
            BinanceDataInterval.ONE_MINUTE,
            "BTCUSDT",
        ),
        datetime.datetime(2018, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    )

    ohlcv = OHLCVUtils.remove_zero_volume(ohlcv)

    batch_indicator = BatchIndicator(
        [
            # Oscillator
            AwesomeOscillator(AwesomeOscillatorConfig()),
            CCI(CCIConfig()),
            CMO(CMOConfig()),
            ConnorsRSI(ConnorsRSIConfig()),
            RSI(RSIConfig()),
            SchaffTrendCycle(SchaffTrendCycleConfig()),
            SMI(SMIConfig()),
            StochasticOscillator(StochasticOscillatorConfig()),
            StochasticRSI(StochasticRSIConfig()),
            TRIX(TRIXConfig()),
            UltimateOscillator(UltimateOscillatorConfig()),
            # Price Channel
            BollingerBands(BollingerBandsConfig()),
            DonchianChannels(DonchianChannelsConfig()),
            # FractalChaosBands(FractalChaosBandsConfig()),
            KeltnerChannels(KeltnerChannelsConfig()),
            # RollingPivotPoints(RollingPivotPointsConfig()),
            # Price Characteristics
            ATR(ATRConfig()),
            # BOP(BOPConfig()),
            ChopinessIndex(ChopinessIndexConfig()),
            PMO(PMOConfig()),
            TSI(TSIConfig()),
            UlcerIndex(UlcerIndexConfig()),
            # Price Trend
            Aroon(AroonConfig()),
            ATRTrailingStop(ATRTrailingStopConfig()),
            ADX(ADXConfig()),
            ElderRay(ElderRayConfig()),
            GatorOscillator(GatorOscillatorConfig()),
            HurstExponent(HurstExponentConfig()),
            # IchimokuCloud(IchimokuCloudConfig()),
            MACD(MACDConfig()),
            SuperTrend(SuperTrendConfig()),
            VortexIndicator(VortexIndicatorConfig()),
            WilliamsAlligator(WilliamsAlligatorConfig()),
            # Stop-Reverse
            ChandelierExit(ChandelierExitConfig()),
            ParabolicSAR(ParabolicSARConfig()),
            VolatilityStop(VolatilityStopConfig()),
            # Volume
            ADL(ADLConfig()),
            ChaikinOscillator(ChaikinOscillatorConfig()),
            CMF(CMFConfig()),
            ForceIndex(ForceIndexConfig()),
            KlingerVolumeOscillator(KlingerVolumeOscillatorConfig()),
            MFI(MFIConfig()),
            OBV(OBVConfig()),
            PVO(PVOConfig()),
        ]
    )

    batch_indicator.calculate(
        OHLCV.validate(ohlcv.collect(engine="streaming")),
        "1m",
        warmup_period=datetime.timedelta(hours=12),
        window_size=datetime.timedelta(weeks=1),
        parquet_path=Path("./btcusdt-2018-2026"),
        return_frame=False,
    )
