from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
from datetime import time
from indicators import calculate_ema, calculate_atr, calculate_rsi, calculate_adx
from advisor import get_advice


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


# ---------- DAY TRADING ----------

class DayTradingStrategy(BaseFXStrategy):
    """
    Day Trading Strategy (FX only) that consults an AI advisor.
    """
    NAME = "AI-Assisted Day Trader"

    def __init__(
        self,
        settings,
        ema_period: int = 50,
        atr_period: int = 20,
        rsi_period: int = 14,
        adx_period: int = 14,
        confidence_threshold: float = 0.6,
        # session/window & risk params inherited from BaseFXStrategy
        session_start: time = time(7, 0),
        session_end: time = time(20, 0),
        pip_size: float = 0.0001,
    ):
        super().__init__(
            settings=settings,
            ema_period=ema_period,
            atr_period=atr_period,
            session_start=session_start,
            session_end=session_end,
            pip_size=pip_size,
        )
        self.rsi_period = rsi_period
        self.adx_period = adx_period
        self.confidence_threshold = confidence_threshold

    def get_required_bars(self) -> Dict[str, int]:
        # Ensure indicator warmup, not just min bars
        need = max(self.ema_period, self.atr_period, self.rsi_period, self.adx_period) + 5
        return {"15m": max(self.settings.general.min_bars_for_trading, need)}

    def decide(self, data: Dict[str, Any]) -> Dict[str, Any]:
        df: pd.DataFrame = data.get("ohlc_15m")
        if df is None or len(df) < self.get_required_bars()["15m"]:
            return self._hold("insufficient data")

        now = df.index[-1]
        if not self._in_session(now):
            return self._hold("outside trading session")

        # --- Calculate indicators ---
        ema_slow = calculate_ema(df, self.ema_period, source_col='close').iloc[-1]
        ema_fast = calculate_ema(df, self.ema_period // 2, source_col='close').iloc[-1]
        atr = calculate_atr(df, self.atr_period).iloc[-1]
        rsi = calculate_rsi(df, self.rsi_period).iloc[-1]
        adx = calculate_adx(df, self.adx_period).iloc[-1]

        if any(pd.isna(v) for v in [ema_slow, ema_fast, atr, rsi, adx]):
            return self._hold("indicators not ready")

        # --- Construct market snapshot for AI ---
        snapshot = {
            "price_bid": float(df['close'].iloc[-1]),
            "spread_pips": self._to_pips(df['high'].iloc[-1] - df['low'].iloc[-1]), # Simplified spread
            "trend_ema_5m": float(ema_slow), # Using 15m slow EMA as trend proxy
            "ema_fast": float(ema_fast),
            "ema_slow": float(ema_slow),
            "rsi": float(rsi),
            "adx": float(adx),
            "atr_pips": self._to_pips(atr),
            "session_hour_utc": now.hour,
            "bot_proposal": {
                "sl_pips": self._to_pips(atr * 2.0),
                "tp_pips": self._to_pips(atr * 3.0)
            }
        }

        # --- Get advice from AI ---
        advice = get_advice(
            snapshot,
            self.settings.advisor.url,
            self.settings.advisor.token
        )

        if not advice:
            return self._hold("advisor request failed")

        action = advice.get("action", "skip")
        confidence = advice.get("confidence", 0.0)
        reason = advice.get("reason", "no reason provided")
        sl_pips = advice.get("sl_pips")
        tp_pips = advice.get("tp_pips")

        comment = f"AI({confidence:.2f}): {action.upper()} - {reason}"

        if action == "skip" or confidence < self.confidence_threshold:
            return self._hold(comment)

        return {
            "action": action,
            "comment": comment,
            "sl_offset": float(sl_pips),
            "tp_offset": float(tp_pips),
        }
