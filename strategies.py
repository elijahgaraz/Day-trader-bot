from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
from datetime import time
from indicators import calculate_ema, calculate_atr


class Strategy(ABC):
    """Abstract base class for trading strategies (FX only)."""
    NAME: str = "Base Strategy"

    @abstractmethod
    def decide(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "action": "hold",
            "comment": f"{self.NAME} not implemented",
            "sl_offset": None,
            "tp_offset": None,
        }

    @abstractmethod
    def get_required_bars(self) -> Dict[str, int]:
        """Returns a dict like {'1m': count} of bars required by the strategy."""
        return {}


# ---------- Shared FX helpers ----------

class BaseFXStrategy(Strategy):
    """
    Base class for FX (fiat) strategies.
    - Enforces session filter
    - Enforces indicator warmup
    - Provides pip-size conversion helpers
    - Provides a permissive volume check (low-volume veto)
    """

    def __init__(
        self,
        settings,
        ema_period: int,
        atr_period: int,
        session_start: time = time(8, 15),
        session_end: time = time(15, 45),
        pip_size: float = 0.0001,  # override per symbol if needed (e.g., JPY pairs = 0.01)
        use_volume_filter: bool = True,
        low_volume_ratio: float = 0.5,  # veto if current volume < 50% of rolling mean
    ):
        self.settings = settings
        self.ema_period = ema_period
        self.atr_period = atr_period
        self.session_start = session_start
        self.session_end = session_end
        self.pip_size = pip_size
        self.use_volume_filter = use_volume_filter
        self.low_volume_ratio = low_volume_ratio

    def get_required_bars(self) -> Dict[str, int]:
        # Ensure indicator warmup, not just min bars
        need = max(self.ema_period, self.atr_period) + 5
        return {"1m": max(self.settings.general.min_bars_for_trading, need)}

    def _in_session(self, ts: pd.Timestamp) -> bool:
        """
        Assumes df.index timezone matches your intended session.
        If your feed is UTC, adjust session or convert index upstream.
        """
        t = ts.time() if isinstance(ts, pd.Timestamp) else ts
        return self.session_start <= t <= self.session_end

    def _hold(self, reason: str) -> Dict[str, Any]:
        return {
            "action": "hold",
            "comment": f"{self.NAME}: {reason}",
            "sl_offset": None,
            "tp_offset": None,
        }

    def _to_pips(self, price_distance: float) -> float:
        # Convert price distance to pips. Example: 0.0012 @ pip_size 0.0001 => 12 pips
        return float(price_distance) / float(self.pip_size)

    def _volume_ok(self, df: pd.DataFrame) -> bool:
        """
        Gentle low-volume veto: allows trades unless volume is abnormally low.
        Looks for 'volume' or 'tick_volume' columns; if none, skips check.
        """
        if not self.use_volume_filter:
            return True

        vol_series = None
        for cand in ("volume", "tick_volume", "TickVolume"):
            if cand in df:
                vol_series = df[cand]
                break

        if vol_series is None or len(vol_series) < self.atr_period:
            return True  # not enough info to judge; don't block

        avg_vol = vol_series.rolling(self.atr_period, min_periods=self.atr_period // 2).mean().iloc[-1]
        curr_vol = vol_series.iloc[-1]

        if pd.isna(avg_vol) or avg_vol <= 0 or pd.isna(curr_vol):
            return True  # can't judge; don't block

        return curr_vol >= self.low_volume_ratio * avg_vol


# ---------- SAFE ----------

class SafeStrategy(BaseFXStrategy):
    """
    Safe (Low-Risk) Trend-Following Scalper (FX only)

    - Session filter (08:15–15:45)
    - Low-volume veto (optional, default on)
    - EMA trend with ATR buffer zone
    - ATR-based SL/TP
    - Simple trailing anchored to entry & favorable excursion
    - Outputs SL/TP as **pips** (consistent across strategies)
    """
    NAME = "Safe (Low-Risk) Trend-Following Scalper"

    def __init__(
        self,
        settings,
        ema_period: int = 20,
        atr_period: int = 14,
        stop_mult: float = 1.0,
        target_mult: float = 1.0,
        buffer_mult: float = 0.2,  # buffer = ATR * buffer_mult
        # session/window & risk params inherited from BaseFXStrategy
        session_start: time = time(8, 15),
        session_end: time = time(15, 45),
        pip_size: float = 0.0001,
        use_volume_filter: bool = True,
        low_volume_ratio: float = 0.5,
    ):
        super().__init__(
            settings=settings,
            ema_period=ema_period,
            atr_period=atr_period,
            session_start=session_start,
            session_end=session_end,
            pip_size=pip_size,
            use_volume_filter=use_volume_filter,
            low_volume_ratio=low_volume_ratio,
        )
        self.stop_mult = stop_mult
        self.target_mult = target_mult
        self.buffer_mult = buffer_mult

        # Trailing/position state
        self.trailing_activated = False
        self.last_action: Optional[str] = None
        self.entry_price: Optional[float] = None
        self.highest_since_entry: Optional[float] = None
        self.lowest_since_entry: Optional[float] = None

    def decide(self, data: Dict[str, Any]) -> Dict[str, Any]:
        df: pd.DataFrame = data.get("ohlc_1m")
        if df is None:
            return self._hold("no data")

        need = self.get_required_bars()["1m"]
        if len(df) < need:
            return self._hold("insufficient data")

        now = df.index[-1]
        if not self._in_session(now):
            return self._hold("outside trading session")

        ema = calculate_ema(df, self.ema_period, source_col='close').iloc[-1]
        close = df['close']
        atr = calculate_atr(df, self.atr_period).iloc[-1]

        if pd.isna(ema) or pd.isna(atr):
            return self._hold("indicators not ready")

        price = float(close.iloc[-1])

        # Volume check (veto only if abnormally low)
        if not self._volume_ok(df):
            return self._hold("very low volume")

        # Buffer zone around EMA
        buffer = float(atr) * self.buffer_mult
        if abs(price - ema) < buffer:
            return self._hold("within buffer zone")

        # Direction
        if price > ema:
            action = "buy"
            comment = f"price {price:.5f} above EMA{self.ema_period} + buffer"
        else:
            action = "sell"
            comment = f"price {price:.5f} below EMA{self.ema_period} - buffer"

        # Reset trailing/entry state on flip
        if self.last_action != action:
            self.trailing_activated = False
            self.entry_price = price
            self.highest_since_entry = price
            self.lowest_since_entry = price
        else:
            # update extremes
            if self.entry_price is not None:
                self.highest_since_entry = max(self.highest_since_entry or price, price)
                self.lowest_since_entry = min(self.lowest_since_entry or price, price)

        self.last_action = action

        # Base distances (price units)
        sl_dist = float(atr) * self.stop_mult
        tp_dist = float(atr) * self.target_mult

        # Trailing activation when price moves further beyond buffer
        if not self.trailing_activated:
            if (action == "buy" and price > ema + 2 * buffer) or \
               (action == "sell" and price < ema - 2 * buffer):
                self.trailing_activated = True
                comment += "; trailing stop activated"

        # Trailing anchored to entry & favorable excursion
        if self.trailing_activated and self.entry_price is not None:
            be_pad = float(atr) * 0.10  # small cushion to avoid premature BE

            if action == "buy" and self.highest_since_entry:
                trailed_sl = max(self.entry_price - be_pad,
                                 self.highest_since_entry - sl_dist)
                new_sl_dist = max(1e-9, price - trailed_sl)
                sl_dist = min(sl_dist, new_sl_dist)

            elif action == "sell" and self.lowest_since_entry:
                trailed_sl = min(self.entry_price + be_pad,
                                 self.lowest_since_entry + sl_dist)
                new_sl_dist = max(1e-9, trailed_sl - price)
                sl_dist = min(sl_dist, new_sl_dist)

        # Convert to pips for execution layer
        sl_pips = self._to_pips(sl_dist)
        tp_pips = self._to_pips(tp_dist)

        return {
            "action": action,
            "comment": f"{self.NAME}: {comment}",
            "sl_offset": float(sl_pips),
            "tp_offset": float(tp_pips),
        }


# ---------- MODERATE ----------

class ModerateStrategy(BaseFXStrategy):
    NAME = "Moderate Trend-Following Scalper"

    def __init__(
        self,
        settings,
        ema_period: int = 20,
        atr_period: int = 14,
        stop_multiplier: float = 1.5,
        target_multiplier: float = 1.0,
        session_start: time = time(8, 15),
        session_end: time = time(15, 45),
        pip_size: float = 0.0001,
        use_volume_filter: bool = True,
        low_volume_ratio: float = 0.5,
    ):
        super().__init__(
            settings, ema_period, atr_period,
            session_start, session_end, pip_size,
            use_volume_filter, low_volume_ratio
        )
        self.stop_multiplier = stop_multiplier
        self.target_multiplier = target_multiplier

    def decide(self, data: Dict[str, Any]) -> Dict[str, Any]:
        df: pd.DataFrame = data.get("ohlc_1m")
        if df is None or len(df) < self.get_required_bars()["1m"]:
            return self._hold("insufficient data")

        if not self._in_session(df.index[-1]):
            return self._hold("outside trading session")

        if not self._volume_ok(df):
            return self._hold("very low volume")

        close = df["close"]
        ema = calculate_ema(close, self.ema_period).iloc[-1]
        atr = calculate_atr(df, self.atr_period).iloc[-1]
        if pd.isna(ema) or pd.isna(atr):
            return self._hold("indicators not ready")

        price = close.iloc[-1]

        if price > ema:
            action = "buy"
            comment = f"{self.NAME}: bullish trend detected"
        elif price < ema:
            action = "sell"
            comment = f"{self.NAME}: bearish trend detected"
        else:
            return self._hold("no clear trend")

        sl_pips = self._to_pips(float(atr) * self.stop_multiplier)
        tp_pips = self._to_pips(float(atr) * self.target_multiplier)
        return {"action": action, "comment": comment, "sl_offset": float(sl_pips), "tp_offset": float(tp_pips)}


# ---------- AGGRESSIVE ----------

class AggressiveStrategy(BaseFXStrategy):
    NAME = "Aggressive Trend-Following Scalper"

    def __init__(
        self,
        settings,
        ema_period: int = 10,
        atr_period: int = 7,
        stop_multiplier: float = 2.0,
        target_multiplier: float = 1.5,
        session_start: time = time(8, 15),
        session_end: time = time(15, 45),
        pip_size: float = 0.0001,
        use_volume_filter: bool = True,
        low_volume_ratio: float = 0.5,
    ):
        super().__init__(
            settings, ema_period, atr_period,
            session_start, session_end, pip_size,
            use_volume_filter, low_volume_ratio
        )
        self.stop_multiplier = stop_multiplier
        self.target_multiplier = target_multiplier

    def decide(self, data: Dict[str, Any]) -> Dict[str, Any]:
        df: pd.DataFrame = data.get("ohlc_1m")
        if df is None or len(df) < self.get_required_bars()["1m"]:
            return self._hold("insufficient data")

        if not self._in_session(df.index[-1]):
            return self._hold("outside trading session")

        if not self._volume_ok(df):
            return self._hold("very low volume")

        close = df["close"]
        ema = calculate_ema(close, self.ema_period).iloc[-1]
        atr = calculate_atr(df, self.atr_period).iloc[-1]
        if pd.isna(ema) or pd.isna(atr):
            return self._hold("indicators not ready")

        price = close.iloc[-1]

        if price > ema:
            action = "buy"
            comment = f"{self.NAME}: going long aggressively"
        elif price < ema:
            action = "sell"
            comment = f"{self.NAME}: going short aggressively"
        else:
            return self._hold("awaiting breakout")

        sl_pips = self._to_pips(float(atr) * self.stop_multiplier)
        tp_pips = self._to_pips(float(atr) * self.target_multiplier)
        return {"action": action, "comment": comment, "sl_offset": float(sl_pips), "tp_offset": float(tp_pips)}


# ---------- MOMENTUM FADE ----------

class MomentumStrategy(BaseFXStrategy):
    NAME = "Momentum Fade Scalper"

    def __init__(
        self,
        settings,
        ema_period: int = 20,
        atr_period: int = 14,
        fade_threshold: float = 1.5,  # ATR multiples
        stop_multiplier: float = 1.0,
        target_multiplier: float = 1.5,
        session_start: time = time(8, 15),
        session_end: time = time(15, 45),
        pip_size: float = 0.0001,
        use_volume_filter: bool = True,
        low_volume_ratio: float = 0.5,
    ):
        super().__init__(
            settings, ema_period, atr_period,
            session_start, session_end, pip_size,
            use_volume_filter, low_volume_ratio
        )
        self.fade_threshold = fade_threshold
        self.stop_multiplier = stop_multiplier
        self.target_multiplier = target_multiplier

    def decide(self, data: Dict[str, Any]) -> Dict[str, Any]:
        df: pd.DataFrame = data.get("ohlc_1m")
        if df is None or len(df) < self.get_required_bars()["1m"]:
            return self._hold("insufficient data")

        if not self._in_session(df.index[-1]):
            return self._hold("outside trading session")

        if not self._volume_ok(df):
            return self._hold("very low volume")

        close = df["close"]
        ema = calculate_ema(close, self.ema_period).iloc[-1]
        atr = calculate_atr(df, self.atr_period).iloc[-1]
        if pd.isna(ema) or pd.isna(atr):
            return self._hold("indicators not ready")

        price = close.iloc[-1]
        diff = price - ema

        if diff > float(atr) * self.fade_threshold:
            action = "sell"
            comment = f"{self.NAME}: fading overextension"
        elif diff < -float(atr) * self.fade_threshold:
            action = "buy"
            comment = f"{self.NAME}: fading downside spike"
        else:
            return self._hold("no fade opportunity")

        sl_pips = self._to_pips(float(atr) * self.stop_multiplier)
        tp_pips = self._to_pips(float(atr) * self.target_multiplier)
        return {"action": action, "comment": comment, "sl_offset": float(sl_pips), "tp_offset": float(tp_pips)}


# ---------- MEAN REVERSION ----------

class MeanReversionStrategy(BaseFXStrategy):
    NAME = "Mean-Reversion Scalper"

    def __init__(
        self,
        settings,
        ema_period: int = 20,
        atr_period: int = 14,
        band_multiplier: float = 2.0,  # ATR multiples
        stop_multiplier: float = 1.0,
        target_multiplier: float = 2.0,
        session_start: time = time(8, 15),
        session_end: time = time(15, 45),
        pip_size: float = 0.0001,
        use_volume_filter: bool = True,
        low_volume_ratio: float = 0.5,
    ):
        super().__init__(
            settings, ema_period, atr_period,
            session_start, session_end, pip_size,
            use_volume_filter, low_volume_ratio
        )
        self.band_multiplier = band_multiplier
        self.stop_multiplier = stop_multiplier
        self.target_multiplier = target_multiplier

    def decide(self, data: Dict[str, Any]) -> Dict[str, Any]:
        df: pd.DataFrame = data.get("ohlc_1m")
        if df is None or len(df) < self.get_required_bars()["1m"]:
            return self._hold("insufficient data")

        if not self._in_session(df.index[-1]):
            return self._hold("outside trading session")

        if not self._volume_ok(df):
            return self._hold("very low volume")

        close = df["close"]
        ema = calculate_ema(close, self.ema_period).iloc[-1]
        atr = calculate_atr(df, self.atr_period).iloc[-1]
        if pd.isna(ema) or pd.isna(atr):
            return self._hold("indicators not ready")

        price = close.iloc[-1]
        upper = float(ema) + float(atr) * self.band_multiplier
        lower = float(ema) - float(atr) * self.band_multiplier

        if price > upper:
            action = "sell"
            comment = f"{self.NAME}: price above upper band"
        elif price < lower:
            action = "buy"
            comment = f"{self.NAME}: price below lower band"
        else:
            return self._hold("within bands")

        sl_pips = self._to_pips(float(atr) * self.stop_multiplier)
        tp_pips = self._to_pips(float(atr) * self.target_multiplier)
        return {"action": action, "comment": comment, "sl_offset": float(sl_pips), "tp_offset": float(tp_pips)}
