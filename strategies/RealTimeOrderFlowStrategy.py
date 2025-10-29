import json
import os
import pandas as pd
import numpy as np
import vectorbt as vbt
from collections import deque
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import threading

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
file_logger = logging.getLogger("orderflow_file")


class RealTimeOrderFlowStrategy:
    """
    Real-time Order Flow Strategy using vectorBT for optimized calculations
    Processes live market data and generates trading signals
    """
    
    def __init__(self, symbol: str, lookback_period: int = 600, initial_cash: float = 100000):
        """
        Initialize real-time order flow strategy with vectorBT
        
        Args:
            symbol: Trading symbol (e.g., "RELIANCE")
            lookback_period: Number of periods to maintain in memory
            initial_cash: Initial capital for vectorBT portfolio simulation
        """
        self.symbol = symbol
        self.lookback_period = lookback_period
        self.initial_cash = initial_cash
        
        # Strategy state
        self.is_initialized = False
        self.current_position = 0
        self.last_signal = None
        self.signal_history = []

        self.volume_traded=0
        
        # Real-time data buffers (using pandas for vectorBT compatibility)
        self.setup_vectorbt_buffers()
        
        # VectorBT portfolio for simulation
        self.setup_vectorbt_portfolio()
        
        # Order flow indicators using vectorBT
        self.setup_vectorbt_indicators()
        
        # Signal generation parameters
        self.setup_strategy_parameters()
        
        logger.info(f"✅ RealTimeOrderFlowStrategy initialized for {symbol} with vectorBT")
    
    def setup_vectorbt_buffers(self):
        """Initialize vectorBT-compatible data buffers"""
        # Main OHLCV DataFrame (continuously updated)
        self.ohlcv_df = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Tick data buffer for delta calculations
        self.tick_buffer = deque(maxlen=5000)
        
        # DOM data buffer
        self.dom_buffer = deque(maxlen=200)
        
        # Volume delta series (for vectorBT processing)
        self.delta_series = pd.Series(dtype=float)
        self.cvd_series = pd.Series(dtype=float)  # Cumulative Volume Delta
        


        # Thread safety
        self.data_lock = threading.RLock()
    
    def setup_vectorbt_portfolio(self):
        """Initialize vectorBT portfolio for strategy simulation"""
        # We'll use vectorBT's portfolio for signal tracking and performance
        self.portfolio = None
        self.signals_df = pd.DataFrame()
        
        # Trade history
        self.trade_history = []
    
    def setup_vectorbt_indicators(self):
        """Initialize vectorBT-based order flow indicators"""
        
        # Volume Delta Indicator (custom vectorBT indicator)
        self.VolumeDelta = vbt.IndicatorFactory(
            class_name='VolumeDelta',
            short_name='vd',
            input_names=['price', 'volume', 'aggressor'],
            param_names=[],
            output_names=['delta']
        ).from_apply_func(self.calculate_volume_delta_vectorbt)
        
        # CVD Indicator
        self.CVD = vbt.IndicatorFactory(
            class_name='CVD',
            short_name='cvd', 
            input_names=['delta'],
            param_names=[],
            output_names=['cvd']
        ).from_apply_func(self.calculate_cvd_vectorbt)
        
        # Order Flow Composite Indicator
        self.OrderFlowComposite = vbt.IndicatorFactory(
            class_name='OrderFlowComposite',
            short_name='ofc',
            input_names=['close', 'high', 'low', 'volume', 'delta', 'cvd'],
            param_names=['delta_window', 'cvd_window'],
            output_names=['delta_ma', 'cvd_ma', 'delta_strength', 'cvd_trend']
        ).from_apply_func(self.calculate_order_flow_composite)
        
        # Current indicator instances
        self.current_indicators = {}
    
    def setup_strategy_parameters(self):
        """Set up strategy parameters optimized for vectorBT"""
        # Volume Delta parameters
        self.delta_threshold = 1000
        self.delta_window = 20
        
        # CVD parameters  
        self.cvd_window = 10
        self.cvd_trend_threshold = 0.1
        
        # Signal parameters
        self.confirmation_ticks = 3
        self.min_confidence = 0.6
        
        # Risk management
        self.position_size = 0.1  # 10% of capital per trade
        self.max_drawdown = 0.05  # 5% max drawdown
    
    # ==========================================================================
    # VECTORBT INDICATOR FUNCTIONS
    # ==========================================================================
    
    @staticmethod
    def calculate_volume_delta_vectorbt(price, volume, aggressor):
        """VectorBT function for volume delta calculation"""
        # This is a simplified version - in practice, you'd use tick-level aggressor data
        price_diff = price.diff()
        
        # Estimate delta based on price movement
        delta = np.where(
            price_diff > 0, volume,           # Buyer initiated
            np.where(price_diff < 0, -volume, 0)  # Seller initiated
        )
        
        return delta
    
    @staticmethod  
    def calculate_cvd_vectorbt(delta):
        """VectorBT function for Cumulative Volume Delta"""
        return delta.cumsum()
    
    @staticmethod
    def calculate_order_flow_composite(close, high, low, volume, delta, cvd, delta_window=20, cvd_window=10):
        print("Inside Order Flow Composite Calculation")
        # --- FIX: Ensure inputs are pandas Series for .rolling() ---
        # The 'close' series is guaranteed to have the correct index from vectorbt.
        # We use its index to convert the NumPy array inputs back to Series.
        if not isinstance(close, pd.Series):
            # Fallback index if close is also an array (shouldn't happen, but safe)
            print("(((((((((((((((((((Close is not a Series, creating default index.)))))))))))))")
            index = pd.RangeIndex(len(close))
            print("Created default index for close:", index)
            close = pd.Series(np.squeeze(close), index=index)
        else:
            index = close.index
        
        print("Index used for re-indexing:", index)
        # Convert delta and cvd back to Series, using the correct index
        delta = pd.Series(np.squeeze(delta), index=index)
        cvd = pd.Series(np.squeeze(cvd), index=index)

        print("Delta Series after re-indexing:")
        # Now .rolling() will work on the re-indexed Series:
        """Composite order flow indicator using vectorBT"""
        delta_ma = delta.rolling(window=delta_window).mean()
        cvd_ma = cvd.rolling(window=cvd_window).mean()
        
        # Delta strength (normalized)
        """ Trading Interpretation:This resulting value is similar to a Z-Score, 
            showing how many standard deviations the current aggression is from 
            the average (implicitly assumed to be zero).
            High Positive Value (e.g., $+3.0$): Indicates the current aggressive 
            buying is 3 standard deviations higher than usual. 
            This points to an abnormal buying frenzy, often signaling a breakout
            attempt or a point of buying exhaustion.
            High Negative Value (e.g., $-2.5$): 
            Indicates the current aggressive selling is far more intense than normal,
            suggesting panic selling or a strong breakdown is occurring."""
    
        delta_std = delta.rolling(window=delta_window).std()
        delta_strength = delta / (delta_std + 1e-6)  # Avoid division by zero
        
        # CVD trend
        """The cvd_trend calculation measures the Rate of Change (RoC) of the Cumulative Volume Delta. 
        It quantifies how fast the total accumulated aggression is building up or unwinding.
        This is an indicator of the momentum of the trend in market aggression:
        Large Positive Value: CVD is rising very quickly, confirming that the 
        current market move (up or down) is backed by rapid accumulation of 
        aggressive volume (strong demand).
        Near Zero Value: CVD is flat, indicating that aggressive buying and selling
        are balanced, and the market is consolidating.
        Changing Value (e.g., positive turning negative): This is a momentum shift.
        It means the accumulated pressure is rapidly shifting from bullish to bearish,
        providing a potential early signal of trend reversal.
                """
        cvd_trend = cvd.diff(cvd_window) / (cvd_window + 1e-6)
        
        return delta_ma, cvd_ma, delta_strength, cvd_trend
    
    # ==========================================================================
    # UTILITY FUNCTIONS
    # ==========================================================================

    @staticmethod
    def align_orderflow_data(ohlcv_df, delta_series, cvd_series, time_range=None, resample_interval='60s'):
        """
        Aligns tick-level delta and cvd data with OHLCV data on a fixed interval.
        
        Parameters:
            ohlcv_df (pd.DataFrame): OHLCV dataframe (indexed by datetime)
            delta_series (pd.Series): Tick-level delta series (indexed by datetime)
            cvd_series (pd.Series): Tick-level cumulative volume delta (indexed by datetime)
            time_range (tuple): Optional (start_time, end_time) to filter data
            resample_interval (str): Resample frequency (e.g., '5S', '1T', etc.)
            
        Returns:
            ohlcv_aligned (pd.DataFrame)
            delta_aligned (pd.Series)
            cvd_aligned (pd.Series)
        """
        # --- 1️⃣ Normalize timestamps ---
        ohlcv_df.index = pd.to_datetime(ohlcv_df.index).tz_localize(None)
        delta_series.index = pd.to_datetime(delta_series.index).tz_localize(None)
        cvd_series.index = pd.to_datetime(cvd_series.index).tz_localize(None)
        print("Timestamps normalized.")
        print(f"OHLCV index sample:\n{ohlcv_df.index[:5]}")
        print(f"Delta series index sample:\n{delta_series.index[:5]}")
        print(f"CVD series index sample:\n{cvd_series.index[:5]}")
        # --- 2️⃣ Optional: restrict to time range ---
        if time_range:
            start, end = time_range
            ohlcv_df = ohlcv_df.loc[start:end]
            delta_series = delta_series.loc[start:end]
            cvd_series = cvd_series.loc[start:end]

        # --- 3️⃣ Resample tick-level data to match OHLCV interval ---
        delta_resampled = delta_series.resample(resample_interval, label='right', closed='right').sum()
        cvd_resampled = cvd_series.resample(resample_interval, label='right', closed='right').last()

        # --- 4️⃣ Find common index across all ---
        common_index = (
            ohlcv_df.index
            .intersection(delta_resampled.index)
            .intersection(cvd_resampled.index)
        ).sort_values()

        # --- 5️⃣ Align all three ---
        ohlcv_aligned = ohlcv_df.loc[common_index]
        delta_aligned = delta_resampled.reindex(common_index)
        cvd_aligned = cvd_resampled.reindex(common_index)

        # --- 6️⃣ Optional debug info ---
        print(f"[align_orderflow_data] Shapes → OHLCV: {len(ohlcv_aligned)}, Delta: {len(delta_aligned)}, CVD: {len(cvd_aligned)}")
        print(f"[align_orderflow_data] Time range: {common_index.min()} → {common_index.max()}")
        print(f"[align_orderflow_data] Sample OHLCV:\n{ohlcv_aligned.head()}")
        return ohlcv_aligned, delta_aligned, cvd_aligned

    
    
    # ==========================================================================
    # REAL-TIME DATA HANDLERS WITH VECTORBT INTEGRATION
    # ==========================================================================
    
    def handle_ohlc_update(self, ohlc_data: Dict):
        """
        Handle OHLC updates with vectorBT processing
        """
        print("***********************Handle OHLC Update********************")
        try:
            with self.data_lock:
                
                if ohlc_data.get('instrument_token') != self.symbol:
                    return
                
                
                # Convert to DataFrame row
                new_row = self.create_ohlcv_row(ohlc_data)
                
                # Update main DataFrame
                self.update_ohlcv_dataframe(new_row)
                
                # Update vectorBT indicators
                self.update_vectorbt_indicators()
                print("Updated vectorBT indicators.")
                # Generate OHLC-based signals
                if len(self.ohlcv_df) >= 10:  # Minimum data for indicators
                    signals = self.generate_ohlc_signals_vectorbt()
                    print("Generated OHLC Signals:", signals)
                    if signals:
                        self.execute_vectorbt_signals(signals, 'ohlc')
                
        except Exception as e:
            logger.error(f"❌ Error handling OHLC update for {self.symbol}: {e}")
    
    def handle_tick_update(self, tick_data: Dict):
        """
        Handle tick updates with vectorBT delta calculation
        """
        #print("***********************Handle Tick Update********************")
        #print(json.dumps(tick_data, indent=2, default=str))
        try:
            with self.data_lock:
                if tick_data.get('instrument_token') != self.symbol:
                    # 🌟 DEBUGGING STATEMENT
                    print(
                        f"DEBUG: Symbol Check. "
                        f"Tick Stock Code: '{tick_data.get('instrument_token')}' | "
                        f"Strategy Symbol: '{self.symbol}' | "
                        f"Result: {tick_data.get('instrument_token') != self.symbol}"
                    )
                    return
                
                #print("***********************Symbol Matched********************")
                # Store tick
                self.tick_buffer.append(tick_data)
                # Update volume delta in real-time, 
                # volume traded so far in a day, and will be usied 
                # to calculate delta_series in update_delta_series function
                tick_delta = self.calculate_tick_delta(tick_data)
                self.update_delta_series(tick_delta, tick_data)
                
                
                
                # print(
                #         f"--- {self.symbol} DELTA SERIES (Top 5 & Last 5) ---\n"
                #         f"{self.delta_series.tail(5)}\n"
                #         f"--- {self.symbol} CVD SERIES (Top 5 & Last 5) ---\n"
                #         f"{self.cvd_series.tail(5)}\n"
                #     )
                # Generate immediate signals
                immediate_signals = self.generate_tick_signals_vectorbt()
                
                # 🌟 ADD THIS PRINT STATEMENT
                print(f"DEBUG: {self.symbol} IMMEDIATE SIGNALS: {immediate_signals}")

                #commenting out to reduce noise
                #if immediate_signals:
                #    self.execute_vectorbt_signals(immediate_signals, 'tick')
                
        except Exception as e:
            logger.error(f"❌ Error handling tick update for {self.symbol}: {e}")
    
    def handle_dom_update(self, dom_data: Dict):
        """
        Handle DOM updates
        """
        try:
            with self.data_lock:
                if dom_data.get('instrument_token') != self.symbol:
                    return
                
                self.dom_buffer.append(dom_data)
                
                # DOM-based signals (less frequent)
                if len(self.dom_buffer) >= 5:
                    dom_signals = self.generate_dom_signals()
                    if dom_signals:
                        self.execute_vectorbt_signals(dom_signals, 'dom')
                
        except Exception as e:
            logger.error(f"❌ Error handling DOM update for {self.symbol}: {e}")
    
    def handle_error(self, error_data: Dict):
        """Handle error events"""
        logger.warning(f"⚠️ Strategy error for {self.symbol}: {error_data.get('type')} - {error_data.get('error')}")
    
    # ==========================================================================
    # VECTORBT DATA MANAGEMENT
    # ==========================================================================
    
    def create_ohlcv_row(self, ohlc_data: Dict) -> pd.Series:
        """Create a pandas Series from OHLC data"""
        return pd.Series({
            'timestamp': ohlc_data.get('timestamp'),    #datetime.now(),
            'open': ohlc_data.get('open', 0),
            'high': ohlc_data.get('high', 0), 
            'low': ohlc_data.get('low', 0),
            'close': ohlc_data.get('close', 0),
            'volume': ohlc_data.get('volume', 0),
            'symbol': ohlc_data.get('instrument_token', self.symbol)
        })
    
    def update_ohlcv_dataframe(self, new_row: pd.Series):
        """Update OHLCV DataFrame with new data"""
        # Convert to DataFrame for concatenation
        new_df = pd.DataFrame([new_row]).set_index('timestamp')
        
        if self.ohlcv_df.empty:
            self.ohlcv_df = new_df
        else:
            # Ensure we maintain the lookback period
            combined = pd.concat([self.ohlcv_df, new_df])
            if len(combined) > self.lookback_period:
                combined = combined.iloc[-self.lookback_period:]
            self.ohlcv_df = combined
    
    def update_delta_series(self, tick_delta: float, tick_data: Dict):
        """Update volume delta series with new tick"""
        #timestamp = datetime.now()
        timestamp = tick_data['exchange_timestamp']
        new_delta = pd.Series([tick_delta], index=[timestamp])
        
        if self.delta_series.empty:
            
            self.delta_series = new_delta
        else:
           
            self.delta_series = pd.concat([self.delta_series, new_delta])
            
        # Update CVD series
        if self.cvd_series.empty:
            self.cvd_series = new_delta.cumsum()
        else:
            # FIX: Calculate the new CVD value by adding the new delta 
            # to the last known absolute CVD total.
            
            # Get the current running total
            last_cvd_value = self.cvd_series.iloc[-1]
            
            # Calculate the new total
            new_cvd_value = last_cvd_value + tick_delta

            # Create a new single-point Series for the total
            new_cvd_point = pd.Series([new_cvd_value], index=[timestamp])

            # Append the new absolute total to the CVD history
            self.cvd_series = pd.concat([self.cvd_series, new_cvd_point])
        

        # delta_log_data = self.delta_series.to_string(
        #     header=True,   # Include the column header (the series name)
        #     index=True,    # Include the index (the timestamp)
        #     float_format='{:.0f}'.format # Format the Delta value as an integer (no decimals)
        #     )

        # cvd_log_data = self.cvd_series.to_string(
        #     header=True,   # Include the column header (the series name)
        #     index=True,    # Include the index (the timestamp)
        #     float_format='{:.0f}'.format # Format the Delta value as an integer (no decimals)
        #     )

        
        # file_logger.info(f"📝 Aligned Delta Bars (Index & Value):\n{delta_log_data}")
        # file_logger.info(f"📝 Aligned CVD Bars (Index & Value):\n{cvd_log_data}")
        
            


        # Maintain buffer size
        if len(self.delta_series) > self.lookback_period:
            self.delta_series = self.delta_series.iloc[-self.lookback_period:]
            self.cvd_series = self.cvd_series.iloc[-self.lookback_period:]
    
    def update_vectorbt_indicators(self):
        """Update all vectorBT indicators with latest data"""
        
        if len(self.ohlcv_df) < 10 or self.delta_series.empty:
            return
        
        try:
            # Align timestamps between OHLCV and delta data
            ohlcv_aligned, delta_aligned, cvd_aligned = self.align_orderflow_data(
                ohlcv_df=self.ohlcv_df,
                delta_series=self.delta_series,
                cvd_series=self.cvd_series,
                time_range=None,
                resample_interval='60s'
            )

            #common_index = self.ohlcv_df.index.intersection(self.delta_series.index)
            delta_log_data = delta_aligned.to_string(
            header=True,   # Include the column header (the series name)
            index=True,    # Include the index (the timestamp)
            float_format='{:.0f}'.format # Format the Delta value as an integer (no decimals)
            )

            cvd_log_data = cvd_aligned.to_string(
            header=True,   # Include the column header (the series name)
            index=True,    # Include the index (the timestamp)
            float_format='{:.0f}'.format # Format the Delta value as an integer (no decimals)
            )

            ohlcv_log_data = ohlcv_aligned.to_string(
            header=True,   # Include the column header (the series name)
            index=True,    # Include the index (the timestamp)
            float_format='{:.0f}'.format # Format the Delta value as an integer (no decimals)
            )

            file_logger.info(f"📝 Aligned Delta Bars (Index & Value):\n{delta_log_data}")
            file_logger.info(f"📝 Aligned CVD Bars (Index & Value):\n{cvd_log_data}")
            file_logger.info(f"📝 Aligned OHLCV Bars (Index & Values):\n{ohlcv_log_data}")
            # if len(common_index) < 5:
            #     return
            
            
            
            # Calculate order flow composite indicator
            ofc_indicator = self.OrderFlowComposite.run(
                close=ohlcv_aligned['close'].squeeze(),
                high=ohlcv_aligned['high'].squeeze(),
                low=ohlcv_aligned['low'].squeeze(), 
                volume=ohlcv_aligned['volume'].squeeze(),
                delta=delta_aligned.squeeze(),
                cvd=cvd_aligned.squeeze(),
                delta_window=self.delta_window,
                cvd_window=self.cvd_window
            )
            
            self.current_indicators['ofc'] = ofc_indicator
            
        except Exception as e:
            logger.error(f"❌ Error updating vectorBT indicators: {e}")
            file_logger.error(f"❌ Error updating vectorBT indicators: {e}")
    
    # ==========================================================================
    # VECTORBT SIGNAL GENERATION
    # ==========================================================================
    
    def generate_ohlc_signals_vectorbt(self) -> Dict:
        """
        Generate signals using vectorBT indicators on OHLC data
        """
        if 'ofc' not in self.current_indicators:
            return {}
        
        try:
            indicators = self.current_indicators['ofc']
            current_price = self.ohlcv_df['close'].iloc[-1]
            
            signals = {}
            
            # Delta-based signals
            delta_signals = self.generate_delta_signals_vectorbt(indicators)
            signals.update(delta_signals)
            
            # CVD-based signals
            cvd_signals = self.generate_cvd_signals_vectorbt(indicators)
            signals.update(cvd_signals)
            
            # Composite signals
            composite_signals = self.generate_composite_signals_vectorbt(indicators)
            signals.update(composite_signals)
            
            if signals:
                signals.update({
                    'timestamp': datetime.now(),
                    'symbol': self.symbol,
                    'price': current_price,
                    'type': 'ohlc',
                    'confidence': self.calculate_signal_confidence(signals)
                })
            
            return signals
            
        except Exception as e:
            logger.error(f"❌ Error generating OHLC signals: {e}")
            return {}
    
    def generate_delta_signals_vectorbt(self, indicators) -> Dict:
        """Generate signals based on volume delta using vectorBT"""
        signals = {}
        
        current_delta = indicators.delta.iloc[-1] if not indicators.delta.empty else 0
        delta_ma = indicators.delta_ma.iloc[-1] if not indicators.delta_ma.empty else 0
        delta_strength = indicators.delta_strength.iloc[-1] if not indicators.delta_strength.empty else 0
        
        # Strong buying pressure
        if (current_delta > self.delta_threshold and 
            current_delta > delta_ma and 
            delta_strength > 1.5):
            
            signals['delta_bullish'] = {
                'direction': 'long',
                'confidence': min(0.8, delta_strength / 3),
                'reason': f'Strong delta: {current_delta:.0f}, strength: {delta_strength:.2f}'
            }
        
        # Strong selling pressure
        elif (current_delta < -self.delta_threshold and 
              current_delta < delta_ma and 
              delta_strength < -1.5):
            
            signals['delta_bearish'] = {
                'direction': 'short', 
                'confidence': min(0.8, abs(delta_strength) / 3),
                'reason': f'Strong delta: {current_delta:.0f}, strength: {delta_strength:.2f}'
            }
        
        file_logger.info(f" Delta_Ma: {delta_ma}, Current_Delta: {current_delta}, Delta_Strength: {delta_strength}")

        return signals
    
    def generate_cvd_signals_vectorbt(self, indicators) -> Dict:
        """Generate signals based on CVD using vectorBT"""
        signals = {}
        
        cvd_trend = indicators.cvd_trend.iloc[-1] if not indicators.cvd_trend.empty else 0
        cvd_ma = indicators.cvd_ma.iloc[-1] if not indicators.cvd_ma.empty else 0
        
        # CVD trending up
        if cvd_trend > self.cvd_trend_threshold:
            signals['cvd_bullish'] = {
                'direction': 'long',
                'confidence': min(0.7, cvd_trend * 5),  # Scale confidence
                'reason': f'CVD trending up: {cvd_trend:.3f}'
            }
        
        # CVD trending down
        elif cvd_trend < -self.cvd_trend_threshold:
            signals['cvd_bearish'] = {
                'direction': 'short',
                'confidence': min(0.7, abs(cvd_trend) * 5),
                'reason': f'CVD trending down: {cvd_trend:.3f}'
            }
        file_logger.info(f" CVD_Ma: {cvd_ma}, CVD_Trend: {cvd_trend}")
        return signals
    
    def generate_composite_signals_vectorbt(self, indicators) -> Dict:
        """Generate composite signals using multiple vectorBT indicators"""
        signals = {}
        
        # Get current values
        current_delta = indicators.delta.iloc[-1] if not indicators.delta.empty else 0
        delta_strength = indicators.delta_strength.iloc[-1] if not indicators.delta_strength.empty else 0
        cvd_trend = indicators.cvd_trend.iloc[-1] if not indicators.cvd_trend.empty else 0
        
        # Strong composite bullish signal
        if (current_delta > self.delta_threshold and 
            delta_strength > 2.0 and 
            cvd_trend > self.cvd_trend_threshold):
            
            signals['composite_bullish'] = {
                'direction': 'long',
                'confidence': 0.85,
                'reason': 'Strong composite bullish: high delta + strong CVD trend'
            }
        
        # Strong composite bearish signal
        elif (current_delta < -self.delta_threshold and 
              delta_strength < -2.0 and 
              cvd_trend < -self.cvd_trend_threshold):
            
            signals['composite_bearish'] = {
                'direction': 'short',
                'confidence': 0.85,
                'reason': 'Strong composite bearish: high delta + weak CVD trend'
            }
        
        return signals
    
    def generate_tick_signals_vectorbt(self) -> Dict:
        """
        Generate immediate signals from tick data using vectorBT
        """
        if len(self.tick_buffer) < 10:
            #print(f"Not enough ticks to generate tick signals. Buffer contents: {self.tick_buffer}")
            return {}
        
        try:
            # Convert recent ticks to DataFrame for vectorBT processing
            ticks_df = self.ticks_to_dataframe(list(self.tick_buffer)[-50:])  # Last 50 ticks
            
            if ticks_df.empty:
                return {}
            
            # Calculate tick-level indicators
           
            
            tick_delta = self.calculate_tick_delta_batch(ticks_df)
            
            signals = {}
            
            # Large trade detection
            large_trade_signal = self.detect_large_trades_vectorbt(ticks_df, tick_delta)
            if large_trade_signal:
                signals['large_trade'] = large_trade_signal
            
            # Delta spike detection
            delta_spike_signal = self.detect_delta_spikes_vectorbt(tick_delta)
            if delta_spike_signal:
                signals['delta_spike'] = delta_spike_signal
            
            
            if signals:
                signals.update({
                    'timestamp': datetime.now(),
                    'symbol': self.symbol,
                    'price': ticks_df['price'].iloc[-1],
                    'type': 'tick'
                })
            
            return signals
            
        except Exception as e:
            logger.error(f"❌ Error generating tick signals: {e}")
            return {}
    
    def ticks_to_dataframe(self, ticks: List[Dict]) -> pd.DataFrame:
        """Convert tick buffer to DataFrame for vectorBT processing"""
        if not ticks:
            return pd.DataFrame()
        
        data = []
        for tick in ticks:
            data.append({
                'timestamp': tick.get('exchange_timestamp'),  # Would use actual tick timestamp in production
                'price': tick.get('last_price', 0),
                'volume': tick.get('last_traded_quantity', 0),
                'symbol': tick.get('instrument_token', self.symbol)
            })
        
        return pd.DataFrame(data).set_index('timestamp')
    
    def calculate_tick_delta_batch(self, ticks_df: pd.DataFrame) -> pd.Series:
        """Calculate delta for a batch of ticks using vectorBT approach"""
        if len(ticks_df) < 2:
            return pd.Series()
        
        price_diff = ticks_df['price'].diff()
        volume = ticks_df['volume']
        
        # Vectorized delta calculation
        delta = np.where(
            price_diff > 0, volume,
            np.where(price_diff < 0, -volume, 0)
        )
        
        return pd.Series(delta, index=ticks_df.index)
    
    def detect_large_trades_vectorbt(self, ticks_df: pd.DataFrame, tick_delta: pd.Series) -> Dict:
        """Detect large trades using vectorBT operations"""
        if tick_delta.empty:
            return {}
        
        current_volume = ticks_df['volume'].iloc[-1]
        avg_volume = ticks_df['volume'].tail(10).mean()
        
        if avg_volume > 0 and current_volume > avg_volume * 3:
            current_delta = tick_delta.iloc[-1]
            return {
                'direction': 'long' if current_delta > 0 else 'short',
                'confidence': 0.6,
                'reason': f'Large trade: {current_volume} vs avg {avg_volume:.0f}'
            }
        
        return {}
    
    def detect_delta_spikes_vectorbt(self, tick_delta: pd.Series) -> Dict:
        """Detect delta spikes using vectorBT operations"""
        if len(tick_delta) < 5:
            return {}
        
        current_delta = tick_delta.iloc[-1]
        recent_deltas = tick_delta.tail(5)
        avg_delta = recent_deltas.abs().mean()
        
        if avg_delta > 0 and abs(current_delta) > avg_delta * 4:
            return {
                'direction': 'long' if current_delta > 0 else 'short',
                'confidence': 0.7,
                'reason': f'Delta spike: {current_delta:.0f} vs avg {avg_delta:.0f}'
            }
        
        return {}
    
    # ==========================================================================
    # VECTORBT SIGNAL EXECUTION AND PORTFOLIO MANAGEMENT
    # ==========================================================================
    
    def execute_vectorbt_signals(self, signals: Dict, source: str):
        """
        Execute signals using vectorBT portfolio simulation
        """
        try:
            # Calculate overall signal direction and confidence
            final_signal = self.combine_signals_vectorbt(signals)
            
            if not final_signal or final_signal['confidence'] < self.min_confidence:
                # 🌟 PRINT STATEMENT ADDED HERE
                print(
                    f"⚠️ LOW CONFIDENCE/NO SIGNAL: {self.symbol} "
                    f"Confidence: {final_signal.get('confidence', 'N/A'):.2f} "
                    f"(Required: {self.min_confidence:.2f}) - Source: {source}"
                )
                return
            
            # Update signal history
            signal_record = {
                'timestamp': datetime.now(),
                'symbol': self.symbol,
                'direction': final_signal['direction'],
                'confidence': final_signal['confidence'],
                'price': signals.get('price', 0),
                'source': source,
                'reasons': final_signal.get('reasons', [])
            }
            
            self.signal_history.append(signal_record)
            last_delta= self.delta_series.iloc[-1]
            last_cvd= self.cvd_series.iloc[-1]

            
            last_tick = pd.DataFrame(list(self.tick_buffer)[-1:])
            
            # Log the signal in file
            file_logger.info(f"🎯 {last_tick.iloc[-1]['exchange_timestamp']} {self.symbol} {final_signal['direction'].upper()} signal "
                       f"(Confidence: {final_signal['confidence']:.2f}) "
                       f"from {source}")
            file_logger.info(f" last_delta: {last_delta}, last_cvd: {last_cvd}")
            # In production, this would connect to your broker API
            # For now, we just track signals
            self.track_signal_execution(signal_record)
            
        except Exception as e:
            logger.error(f"❌ Error executing vectorBT signals: {e}")
    
    def combine_signals_vectorbt(self, signals: Dict) -> Dict:
        """
        Combine multiple signals using vectorBT-inspired weighting
        """
        if not signals:
            return None
        
        long_confidence = 0
        short_confidence = 0
        reasons = []
        
        for signal_name, signal_data in signals.items():
            if signal_name in ['timestamp', 'symbol', 'price', 'type', 'confidence']:
                continue
                
            confidence = signal_data.get('confidence', 0)
            direction = signal_data.get('direction', '')
            reason = signal_data.get('reason', '')
            
            if direction == 'long':
                long_confidence += confidence
            elif direction == 'short':
                short_confidence += confidence
                
            if reason:
                reasons.append(f"{signal_name}: {reason}")
        
        # Determine overall direction
        if long_confidence > short_confidence and long_confidence > 0:
            return {
                'direction': 'long',
                'confidence': min(1.0, long_confidence),
                'reasons': reasons
            }
        elif short_confidence > long_confidence and short_confidence > 0:
            return {
                'direction': 'short', 
                'confidence': min(1.0, short_confidence),
                'reasons': reasons
            }
        
        return None
    
    def track_signal_execution(self, signal_record: Dict):
        """Track signal execution for performance analysis"""
        self.trade_history.append(signal_record)
        
        # Update current position (simulated)
        if signal_record['direction'] == 'long':
            self.current_position = 1
        elif signal_record['direction'] == 'short':
            self.current_position = -1
    
    def calculate_signal_confidence(self, signals: Dict) -> float:
        """Calculate overall signal confidence"""
        confidences = []
        
        for signal_name, signal_data in signals.items():
            if signal_name in ['timestamp', 'symbol', 'price', 'type', 'confidence']:
                continue
            confidences.append(signal_data.get('confidence', 0))
        
        return np.mean(confidences) if confidences else 0
    
    def calculate_tick_delta(self, tick_data: Dict) -> float:
        """Calculate delta for individual tick"""
        """Calculates the Volume Delta for a single tick based on trade price vs. Best Bid/Ask. """
        try:
            # Extract trade information
            last_price = tick_data['last_price']
            #last_qty = tick_data['last_traded_quantity']
            #changed from last_traded_quantity to volume traded, while updating the delta_series
            #
            last_qty = tick_data['volume_traded']  - self.volume_traded
            self.volume_traded = tick_data['volume_traded']
            # Extract Level 1 (Best) Bid and Ask prices
            best_bid_price = tick_data['depth']['buy'][0]['price']
            best_ask_price = tick_data['depth']['sell'][0]['price']
            
            volume_delta = 0.0
            
            # 1. Check for Market Buy (Trade at Ask or higher)
            if last_price >= best_ask_price:
                volume_delta = last_qty
            
            # 2. Check for Market Sell (Trade at Bid or lower)
            elif last_price <= best_bid_price:
                volume_delta = -last_qty
                
            # 3. Mid-point trade is zero (Volume Delta is 0)
            # else: volume_delta remains 0.0
                
            return volume_delta
        
        except Exception as e:
            # Handle cases where depth might be missing or list index is out of range
            print(f"Error calculating delta: {e}")
            return 0.0
            
        
    
    def generate_dom_signals(self) -> Dict:
        """Generate DOM-based signals"""
        # DOM signal implementation (similar to previous version)
        return {}
    
    # ==========================================================================
    # STRATEGY REGISTRATION AND UTILITY METHODS
    # ==========================================================================
    
    def register_with_datastream(self, data_stream):
        """Register with DataLiveStream instance"""
        try:
            data_stream.register_ohlc_callback(self.handle_ohlc_update)
            data_stream.register_tick_callback(self.handle_tick_update)
            data_stream.register_dom_callback(self.handle_dom_update)
            data_stream.register_error_callback(self.handle_error)
            
            logger.info(f"✅ VectorBT strategy registered for {self.symbol}")
            
        except Exception as e:
            logger.error(f"❌ Failed to register vectorBT strategy: {e}")
    
    def unregister_from_datastream(self, data_stream):
        """Unregister from DataLiveStream instance"""
        try:
            data_stream.unregister_callback(self.handle_ohlc_update, 'ohlc')
            data_stream.unregister_callback(self.handle_tick_update, 'tick')
            data_stream.unregister_callback(self.handle_dom_update, 'dom')
            data_stream.unregister_callback(self.handle_error, 'error')
            
        except Exception as e:
            logger.error(f"❌ Failed to unregister vectorBT strategy: {e}")
    
    def get_strategy_status(self) -> Dict:
        """Get current strategy status"""
        return {
            'symbol': self.symbol,
            'position': self.current_position,
            'ohlcv_records': len(self.ohlcv_df),
            'tick_records': len(self.tick_buffer),
            'dom_records': len(self.dom_buffer),
            'signals_generated': len(self.signal_history),
            'current_indicators': list(self.current_indicators.keys())
        }
    
    def get_performance_summary(self) -> Dict:
        """Get strategy performance summary"""
        if not self.signal_history:
            return {'total_signals': 0, 'recent_signals': []}
        
        recent_signals = self.signal_history[-10:]  # Last 10 signals
        
        return {
            'total_signals': len(self.signal_history),
            'recent_signals': recent_signals,
            'current_position': self.current_position
        }