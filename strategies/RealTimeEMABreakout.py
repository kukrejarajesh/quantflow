import os
import pandas as pd
import numpy as np
from datetime import datetime, time
import time as time_module
from pathlib import Path
import json
from collections import defaultdict

# ==============================================================================
# CONFIGURATION
# ==============================================================================

class StrategyConfig:
    """Configuration for the EMA Breakout Strategy"""
    
    # Data paths
    REALTIME_DATA_ROOT = r"F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\quarterly"
    
    # Strategy parameters
    EMA_PERIOD = 9
    STOP_LOSS_PCT = 2.5
    TRAILING_STOP_PCT = 2.5
    
    # Trading hours
    MARKET_OPEN = time(9, 15)
    MARKET_CLOSE = time(15, 30)
    
    # Position management
    MAX_POSITIONS = 5  # Maximum concurrent positions
    POSITION_SIZE_PCT = 20  # % of capital per position
    INITIAL_CAPITAL = 100000
    
    # Monitoring
    CHECK_INTERVAL_SECONDS = 60  # Check for signals every 60 seconds
    
    # Output paths
    OUTPUT_DIR = "strategy_output"
    SIGNALS_LOG = os.path.join(OUTPUT_DIR, "signals_log.parquet")
    POSITIONS_LOG = os.path.join(OUTPUT_DIR, "active_positions.parquet")
    TRADES_LOG = os.path.join(OUTPUT_DIR, "completed_trades.parquet")
    POSITIONS_HISTORY = os.path.join(OUTPUT_DIR, "positions_history.parquet")  # All position snapshots


# ==============================================================================
# POSITION TRACKING
# ==============================================================================

class Position:
    """Represents an open trading position"""
    
    def __init__(self, instrument_token, entry_price, entry_time, quantity, 
                 stop_loss, trailing_stop, position_type='LONG'):
        self.instrument_token = instrument_token
        self.entry_price = entry_price
        self.entry_time = entry_time
        self.quantity = quantity
        self.stop_loss = stop_loss
        self.trailing_stop = trailing_stop
        self.position_type = position_type
        self.highest_price = entry_price  # For trailing stop
        self.current_price = entry_price
        self.unrealized_pnl = 0.0
        self.status = 'OPEN'
    
    def update_price(self, current_price):
        """Update current price and recalculate PnL and trailing stop"""
        self.current_price = current_price
        
        # Update highest price for trailing stop
        if current_price > self.highest_price:
            self.highest_price = current_price
            # Update trailing stop (moves up with price)
            new_trailing_stop = self.highest_price * (1 - self.trailing_stop / 100)
            if new_trailing_stop > self.stop_loss:
                self.stop_loss = new_trailing_stop
        
        # Calculate unrealized PnL
        self.unrealized_pnl = (current_price - self.entry_price) * self.quantity
    
    def check_exit_conditions(self):
        """Check if position should be exited"""
        # Check fixed stop loss
        if self.current_price <= self.stop_loss:
            return True, "STOP_LOSS"
        
        return False, None
    
    def to_dict(self):
        """Convert position to dictionary for saving"""
        return {
            'instrument_token': self.instrument_token,
            'entry_price': self.entry_price,
            'entry_time': str(self.entry_time),
            'quantity': self.quantity,
            'stop_loss': self.stop_loss,
            'trailing_stop': self.trailing_stop,
            'position_type': self.position_type,
            'highest_price': self.highest_price,
            'current_price': self.current_price,
            'unrealized_pnl': self.unrealized_pnl,
            'status': self.status
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create position from dictionary"""
        pos = cls(
            instrument_token=data['instrument_token'],
            entry_price=data['entry_price'],
            entry_time=pd.to_datetime(data['entry_time']),
            quantity=data['quantity'],
            stop_loss=data['stop_loss'],
            trailing_stop=data['trailing_stop'],
            position_type=data['position_type']
        )
        pos.highest_price = data['highest_price']
        pos.current_price = data['current_price']
        pos.unrealized_pnl = data['unrealized_pnl']
        pos.status = data['status']
        return pos


# ==============================================================================
# REAL-TIME EMA BREAKOUT STRATEGY
# ==============================================================================

class EMABreakoutStrategy:
    """
    Real-time EMA Breakout Strategy
    Monitors live data and generates entry/exit signals
    """
    
    def __init__(self, config, instruments):
        self.config = config
        self.instruments = instruments
        self.positions = {}  # {instrument_token: Position}
        self.capital_available = config.INITIAL_CAPITAL
        self.signals_history = []
        self.trades_history = []
        self.positions_snapshots = []  # For historical position tracking
        
        # Create output directory
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        
        # Load existing positions and history if any
        self.load_positions()
        self.load_historical_data()
    
    def load_latest_data(self, instrument_token, date_folder):
        """
        Load the latest 1-minute rolled-up data for an instrument
        """
        file_path = os.path.join(self.config.REALTIME_DATA_ROOT, date_folder, 
                                f"{instrument_token}.parquet")
        
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_parquet(file_path)
            df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])
            df = df.sort_values('exchange_timestamp')
            
            # Filter to market hours only
            df = df[
                (df['exchange_timestamp'].dt.time >= self.config.MARKET_OPEN) &
                (df['exchange_timestamp'].dt.time <= self.config.MARKET_CLOSE)
            ]
            
            return df
        
        except Exception as e:
            print(f"❌ Error loading data for {instrument_token}: {e}")
            return None
    
    def calculate_ema(self, close_prices, period):
        """Calculate EMA"""
        return close_prices.ewm(span=period, adjust=False).mean()
    
    def check_entry_signal(self, df):
        """
        Check if there's an entry signal (price crosses above EMA)
        Returns: (signal_exists, signal_price, signal_time)
        """
        if len(df) < self.config.EMA_PERIOD + 1:
            return False, None, None
        
        close = df['ohlc_close']
        ema = self.calculate_ema(close, self.config.EMA_PERIOD)
        
        # Check last candle for crossover
        if len(df) >= 2:
            prev_close = close.iloc[-2]
            prev_ema = ema.iloc[-2]
            current_close = close.iloc[-1]
            current_ema = ema.iloc[-1]
            
            # Entry: Price crosses above EMA
            if prev_close <= prev_ema and current_close > current_ema:
                signal_time = df['exchange_timestamp'].iloc[-1]
                return True, current_close, signal_time
        
        return False, None, None
    
    def calculate_position_size(self, entry_price):
        """Calculate position size based on available capital"""
        position_capital = self.capital_available * (self.config.POSITION_SIZE_PCT / 100)
        quantity = int(position_capital / entry_price)
        return quantity
    
    def generate_entry_signal(self, instrument_token, entry_price, signal_time):
        """
        Generate entry signal and create position
        """
        # Check if we can take new position
        if len(self.positions) >= self.config.MAX_POSITIONS:
            print(f"⚠️  Max positions reached. Skipping {instrument_token}")
            return None
        
        # Check if already in position
        if instrument_token in self.positions:
            print(f"⚠️  Already in position for {instrument_token}")
            return None
        
        # Calculate position size
        quantity = self.calculate_position_size(entry_price)
        
        if quantity <= 0:
            print(f"⚠️  Insufficient capital for {instrument_token}")
            return None
        
        # Calculate stop loss
        stop_loss = entry_price * (1 - self.config.STOP_LOSS_PCT / 100)
        
        # Create position
        position = Position(
            instrument_token=instrument_token,
            entry_price=entry_price,
            entry_time=signal_time,
            quantity=quantity,
            stop_loss=stop_loss,
            trailing_stop=self.config.TRAILING_STOP_PCT,
            position_type='LONG'
        )
        
        # Add to positions
        self.positions[instrument_token] = position
        
        # Update capital
        self.capital_available -= entry_price * quantity
        
        # Log signal
        signal = {
            'timestamp': signal_time,
            'instrument_token': instrument_token,
            'signal_type': 'ENTRY',
            'price': entry_price,
            'quantity': quantity,
            'stop_loss': stop_loss,
            'capital_used': entry_price * quantity,
            'capital_available': self.capital_available
        }
        self.signals_history.append(signal)
        
        print(f"\n🟢 ENTRY SIGNAL: {instrument_token}")
        print(f"   Price: ₹{entry_price:.2f}")
        print(f"   Quantity: {quantity}")
        print(f"   Stop Loss: ₹{stop_loss:.2f}")
        print(f"   Investment: ₹{entry_price * quantity:.2f}")
        
        return position
    
    def check_exit_signals(self, date_folder):
        """
        Check all open positions for exit conditions
        """
        positions_to_exit = []
        
        for instrument_token, position in self.positions.items():
            # Load latest data
            df = self.load_latest_data(instrument_token, date_folder)
            
            if df is None or len(df) == 0:
                continue
            
            # Update position with current price
            current_price = df['ohlc_close'].iloc[-1]
            current_time = df['exchange_timestamp'].iloc[-1]
            position.update_price(current_price)
            
            # Check exit conditions
            should_exit, exit_reason = position.check_exit_conditions()
            
            if should_exit:
                positions_to_exit.append((instrument_token, current_price, 
                                        current_time, exit_reason))
        
        # Execute exits
        for instrument_token, exit_price, exit_time, exit_reason in positions_to_exit:
            self.execute_exit(instrument_token, exit_price, exit_time, exit_reason)
    
    def execute_exit(self, instrument_token, exit_price, exit_time, exit_reason):
        """
        Execute exit for a position
        """
        position = self.positions[instrument_token]
        
        # Calculate realized PnL
        realized_pnl = (exit_price - position.entry_price) * position.quantity
        realized_pnl_pct = (exit_price / position.entry_price - 1) * 100
        
        # Return capital
        self.capital_available += exit_price * position.quantity
        
        # Log trade
        trade = {
            'instrument_token': instrument_token,
            'entry_time': position.entry_time,
            'entry_price': position.entry_price,
            'exit_time': exit_time,
            'exit_price': exit_price,
            'quantity': position.quantity,
            'realized_pnl': realized_pnl,
            'realized_pnl_pct': realized_pnl_pct,
            'exit_reason': exit_reason,
            'holding_period': (exit_time - position.entry_time).total_seconds() / 3600  # hours
        }
        self.trades_history.append(trade)
        
        # Log signal
        signal = {
            'timestamp': exit_time,
            'instrument_token': instrument_token,
            'signal_type': 'EXIT',
            'price': exit_price,
            'quantity': position.quantity,
            'exit_reason': exit_reason,
            'pnl': realized_pnl,
            'pnl_pct': realized_pnl_pct,
            'capital_available': self.capital_available
        }
        self.signals_history.append(signal)
        
        print(f"\n🔴 EXIT SIGNAL: {instrument_token}")
        print(f"   Exit Price: ₹{exit_price:.2f}")
        print(f"   Exit Reason: {exit_reason}")
        print(f"   PnL: ₹{realized_pnl:.2f} ({realized_pnl_pct:+.2f}%)")
        print(f"   Holding Period: {trade['holding_period']:.1f} hours")
        
        # Remove position
        del self.positions[instrument_token]
    
    def scan_instruments(self, date_folder):
        """
        Scan all instruments for entry signals
        """
        print(f"\n{'='*80}")
        print(f"🔍 Scanning {len(self.instruments)} instruments for signals...")
        print(f"{'='*80}")
        
        for instrument_token in self.instruments:
            # Skip if already in position
            if instrument_token in self.positions:
                continue
            
            # Load data
            df = self.load_latest_data(instrument_token, date_folder)
            
            if df is None or len(df) == 0:
                continue
            
            # Check for entry signal
            has_signal, entry_price, signal_time = self.check_entry_signal(df)
            
            if has_signal:
                self.generate_entry_signal(instrument_token, entry_price, signal_time)
    
    def update_positions_display(self):
        """Display current positions status"""
        if not self.positions:
            print("\n📊 No active positions")
            return
        
        print(f"\n{'='*80}")
        print(f"📊 ACTIVE POSITIONS: {len(self.positions)}/{self.config.MAX_POSITIONS}")
        print(f"{'='*80}")
        
        total_invested = 0
        total_pnl = 0
        
        for instrument_token, position in self.positions.items():
            invested = position.entry_price * position.quantity
            total_invested += invested
            total_pnl += position.unrealized_pnl
            
            print(f"\n{instrument_token}:")
            print(f"  Entry: ₹{position.entry_price:.2f} @ {position.entry_time.strftime('%H:%M')}")
            print(f"  Current: ₹{position.current_price:.2f}")
            print(f"  Stop Loss: ₹{position.stop_loss:.2f}")
            print(f"  Qty: {position.quantity}")
            print(f"  Unrealized P&L: ₹{position.unrealized_pnl:+.2f} ({position.unrealized_pnl/invested*100:+.2f}%)")
        
        print(f"\n{'='*80}")
        print(f"💰 Total Invested: ₹{total_invested:.2f}")
        print(f"💵 Total Unrealized P&L: ₹{total_pnl:+.2f}")
        print(f"💳 Available Capital: ₹{self.capital_available:.2f}")
        print(f"{'='*80}")
    
    def save_positions(self):
        """Save active positions to Parquet"""
        if not self.positions:
            # Save empty DataFrame to indicate no positions
            empty_df = pd.DataFrame(columns=[
                'instrument_token', 'entry_price', 'entry_time', 'quantity',
                'stop_loss', 'trailing_stop', 'position_type', 'highest_price',
                'current_price', 'unrealized_pnl', 'status', 'last_updated'
            ])
            empty_df.to_parquet(self.config.POSITIONS_LOG, index=False)
            return
        
        positions_data = []
        for token, pos in self.positions.items():
            pos_dict = pos.to_dict()
            pos_dict['last_updated'] = datetime.now()
            positions_data.append(pos_dict)
        
        df = pd.DataFrame(positions_data)
        df.to_parquet(self.config.POSITIONS_LOG, index=False)
    
    def load_positions(self):
        """Load active positions from Parquet"""
        if os.path.exists(self.config.POSITIONS_LOG):
            try:
                df = pd.read_parquet(self.config.POSITIONS_LOG)
                
                if len(df) == 0:
                    print("ℹ️  No existing positions found")
                    return
                
                self.positions = {}
                for _, row in df.iterrows():
                    pos = Position(
                        instrument_token=int(row['instrument_token']),
                        entry_price=row['entry_price'],
                        entry_time=pd.to_datetime(row['entry_time']),
                        quantity=row['quantity'],
                        stop_loss=row['stop_loss'],
                        trailing_stop=row['trailing_stop'],
                        position_type=row['position_type']
                    )
                    pos.highest_price = row['highest_price']
                    pos.current_price = row['current_price']
                    pos.unrealized_pnl = row['unrealized_pnl']
                    pos.status = row['status']
                    self.positions[int(row['instrument_token'])] = pos
                
                print(f"✅ Loaded {len(self.positions)} existing positions")
                
                # Update capital based on positions
                total_invested = sum(
                    p.entry_price * p.quantity 
                    for p in self.positions.values()
                )
                self.capital_available = self.config.INITIAL_CAPITAL - total_invested
                
            except Exception as e:
                print(f"⚠️  Could not load positions: {e}")
    
    def save_signals(self):
        """Save signals history to Parquet (append mode)"""
        if not self.signals_history:
            return
        
        new_signals_df = pd.DataFrame(self.signals_history)
        
        # Append to existing file if it exists
        if os.path.exists(self.config.SIGNALS_LOG):
            try:
                existing_df = pd.read_parquet(self.config.SIGNALS_LOG)
                combined_df = pd.concat([existing_df, new_signals_df], ignore_index=True)
                combined_df.drop_duplicates(
                    subset=['timestamp', 'instrument_token', 'signal_type'],
                    keep='last',
                    inplace=True
                )
                combined_df.to_parquet(self.config.SIGNALS_LOG, index=False)
            except Exception as e:
                print(f"⚠️  Error appending signals: {e}")
                new_signals_df.to_parquet(self.config.SIGNALS_LOG, index=False)
        else:
            new_signals_df.to_parquet(self.config.SIGNALS_LOG, index=False)
        
        # Clear the in-memory buffer after saving
        self.signals_history = []
    
    def save_trades(self):
        """Save completed trades to Parquet (append mode)"""
        if not self.trades_history:
            return
        
        new_trades_df = pd.DataFrame(self.trades_history)
        
        # Append to existing file if it exists
        if os.path.exists(self.config.TRADES_LOG):
            try:
                existing_df = pd.read_parquet(self.config.TRADES_LOG)
                combined_df = pd.concat([existing_df, new_trades_df], ignore_index=True)
                combined_df.drop_duplicates(
                    subset=['instrument_token', 'entry_time', 'exit_time'],
                    keep='last',
                    inplace=True
                )
                combined_df.to_parquet(self.config.TRADES_LOG, index=False)
            except Exception as e:
                print(f"⚠️  Error appending trades: {e}")
                new_trades_df.to_parquet(self.config.TRADES_LOG, index=False)
        else:
            new_trades_df.to_parquet(self.config.TRADES_LOG, index=False)
        
        # Clear the in-memory buffer after saving
        self.trades_history = []
    
    def save_positions_snapshot(self):
        """
        Save a snapshot of all current positions for historical tracking
        This creates a time-series of position states for analysis
        """
        if not self.positions:
            return
        
        snapshot_time = datetime.now()
        snapshots = []
        
        for token, pos in self.positions.items():
            snapshot = pos.to_dict()
            snapshot['snapshot_time'] = snapshot_time
            snapshots.append(snapshot)
        
        new_snapshot_df = pd.DataFrame(snapshots)
        
        # Append to positions history
        if os.path.exists(self.config.POSITIONS_HISTORY):
            try:
                existing_df = pd.read_parquet(self.config.POSITIONS_HISTORY)
                combined_df = pd.concat([existing_df, new_snapshot_df], ignore_index=True)
                combined_df.to_parquet(self.config.POSITIONS_HISTORY, index=False)
            except Exception as e:
                print(f"⚠️  Error appending position snapshot: {e}")
                new_snapshot_df.to_parquet(self.config.POSITIONS_HISTORY, index=False)
        else:
            new_snapshot_df.to_parquet(self.config.POSITIONS_HISTORY, index=False)
    
    def load_historical_data(self):
        """Load historical signals and trades for analysis"""
        # Load signals
        if os.path.exists(self.config.SIGNALS_LOG):
            try:
                signals_df = pd.read_parquet(self.config.SIGNALS_LOG)
                print(f"ℹ️  Loaded {len(signals_df)} historical signals")
            except Exception as e:
                print(f"⚠️  Could not load signals history: {e}")
        
        # Load trades
        if os.path.exists(self.config.TRADES_LOG):
            try:
                trades_df = pd.read_parquet(self.config.TRADES_LOG)
                print(f"ℹ️  Loaded {len(trades_df)} historical trades")
            except Exception as e:
                print(f"⚠️  Could not load trades history: {e}")
    
    def run_single_scan(self, date_folder):
        """
        Run one complete scan cycle:
        1. Check exit signals for existing positions
        2. Scan for new entry signals
        3. Update display
        4. Save state (positions, signals, trades)
        5. Save position snapshot for historical tracking
        """
        print(f"\n{'#'*80}")
        print(f"⏰ Scan Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#'*80}")
        
        # Check exits first
        self.check_exit_signals(date_folder)
        
        # Scan for new entries
        self.scan_instruments(date_folder)
        
        # Update display
        self.update_positions_display()
        
        # Save current state
        self.save_positions()
        self.save_signals()
        self.save_trades()
        
        # Save position snapshot for historical tracking
        self.save_positions_snapshot()
    
    def run_continuous(self, date_folder):
        """
        Run strategy continuously during market hours
        """
        print(f"\n{'='*80}")
        print(f"🚀 EMA BREAKOUT STRATEGY - LIVE MONITORING")
        print(f"{'='*80}")
        print(f"Instruments: {len(self.instruments)}")
        print(f"Capital: ₹{self.config.INITIAL_CAPITAL:,.0f}")
        print(f"Max Positions: {self.config.MAX_POSITIONS}")
        print(f"Check Interval: {self.config.CHECK_INTERVAL_SECONDS}s")
        print(f"{'='*80}")
        
        try:
            while True:
                current_time = datetime.now().time()
                
                # Check if within market hours
                if self.config.MARKET_OPEN <= current_time <= self.config.MARKET_CLOSE:
                    self.run_single_scan(date_folder)
                else:
                    print(f"\n⏸️  Outside market hours. Sleeping...")
                
                # Wait before next scan
                print(f"\n💤 Next scan in {self.config.CHECK_INTERVAL_SECONDS} seconds...")
                time_module.sleep(self.config.CHECK_INTERVAL_SECONDS)
        
        except KeyboardInterrupt:
            print("\n\n⛔ Strategy stopped by user")
            self.save_positions()
            self.save_signals()
            self.save_trades()
            print("✅ State saved")


# ==============================================================================
# ANALYSIS UTILITIES
# ==============================================================================

class StrategyAnalyzer:
    """
    Analyze historical trading data stored in Parquet files
    """
    
    def __init__(self, output_dir="strategy_output"):
        self.output_dir = output_dir
        self.signals_df = None
        self.trades_df = None
        self.positions_history_df = None
        
        self.load_all_data()
    
    def load_all_data(self):
        """Load all parquet files"""
        signals_path = os.path.join(self.output_dir, "signals_log.parquet")
        trades_path = os.path.join(self.output_dir, "completed_trades.parquet")
        positions_path = os.path.join(self.output_dir, "positions_history.parquet")
        
        if os.path.exists(signals_path):
            self.signals_df = pd.read_parquet(signals_path)
            print(f"✅ Loaded {len(self.signals_df)} signals")
        
        if os.path.exists(trades_path):
            self.trades_df = pd.read_parquet(trades_path)
            self.trades_df['entry_time'] = pd.to_datetime(self.trades_df['entry_time'])
            self.trades_df['exit_time'] = pd.to_datetime(self.trades_df['exit_time'])
            print(f"✅ Loaded {len(self.trades_df)} completed trades")
        
        if os.path.exists(positions_path):
            self.positions_history_df = pd.read_parquet(positions_path)
            self.positions_history_df['snapshot_time'] = pd.to_datetime(
                self.positions_history_df['snapshot_time']
            )
            print(f"✅ Loaded {len(self.positions_history_df)} position snapshots")
    
    def get_performance_summary(self):
        """Generate overall performance summary"""
        if self.trades_df is None or len(self.trades_df) == 0:
            print("⚠️  No trades found")
            return None
        
        df = self.trades_df
        
        summary = {
            'total_trades': len(df),
            'winning_trades': (df['realized_pnl'] > 0).sum(),
            'losing_trades': (df['realized_pnl'] <= 0).sum(),
            'win_rate': (df['realized_pnl'] > 0).mean() * 100,
            'total_pnl': df['realized_pnl'].sum(),
            'avg_pnl': df['realized_pnl'].mean(),
            'avg_win': df[df['realized_pnl'] > 0]['realized_pnl'].mean(),
            'avg_loss': df[df['realized_pnl'] <= 0]['realized_pnl'].mean(),
            'largest_win': df['realized_pnl'].max(),
            'largest_loss': df['realized_pnl'].min(),
            'avg_holding_hours': df['holding_period'].mean(),
            'profit_factor': abs(
                df[df['realized_pnl'] > 0]['realized_pnl'].sum() / 
                df[df['realized_pnl'] <= 0]['realized_pnl'].sum()
            ) if (df['realized_pnl'] <= 0).any() else float('inf')
        }
        
        return summary
    
    def print_performance_report(self):
        """Print detailed performance report"""
        summary = self.get_performance_summary()
        
        if summary is None:
            return
        
        print("\n" + "="*80)
        print("📊 STRATEGY PERFORMANCE REPORT")
        print("="*80)
        
        print(f"\n📈 OVERALL STATISTICS:")
        print(f"  Total Trades: {summary['total_trades']}")
        print(f"  Winning Trades: {summary['winning_trades']}")
        print(f"  Losing Trades: {summary['losing_trades']}")
        print(f"  Win Rate: {summary['win_rate']:.2f}%")
        
        print(f"\n💰 P&L ANALYSIS:")
        print(f"  Total P&L: ₹{summary['total_pnl']:+,.2f}")
        print(f"  Average P&L per Trade: ₹{summary['avg_pnl']:+,.2f}")
        print(f"  Average Win: ₹{summary['avg_win']:+,.2f}")
        print(f"  Average Loss: ₹{summary['avg_loss']:+,.2f}")
        print(f"  Largest Win: ₹{summary['largest_win']:+,.2f}")
        print(f"  Largest Loss: ₹{summary['largest_loss']:+,.2f}")
        print(f"  Profit Factor: {summary['profit_factor']:.2f}")
        
        print(f"\n⏱️  TIMING:")
        print(f"  Average Holding Period: {summary['avg_holding_hours']:.1f} hours")
        
        print("="*80)
    
    def get_trades_by_instrument(self):
        """Get performance breakdown by instrument"""
        if self.trades_df is None or len(self.trades_df) == 0:
            return None
        
        instrument_stats = self.trades_df.groupby('instrument_token').agg({
            'realized_pnl': ['count', 'sum', 'mean'],
            'realized_pnl_pct': 'mean',
            'holding_period': 'mean'
        }).round(2)
        
        instrument_stats.columns = ['trades', 'total_pnl', 'avg_pnl', 'avg_pnl_pct', 'avg_holding_hours']
        instrument_stats = instrument_stats.sort_values('total_pnl', ascending=False)
        
        return instrument_stats
    
    def get_trades_by_date(self):
        """Get daily performance"""
        if self.trades_df is None or len(self.trades_df) == 0:
            return None
        
        df = self.trades_df.copy()
        df['exit_date'] = df['exit_time'].dt.date
        
        daily_stats = df.groupby('exit_date').agg({
            'realized_pnl': ['count', 'sum', 'mean'],
            'realized_pnl_pct': 'mean'
        }).round(2)
        
        daily_stats.columns = ['trades', 'total_pnl', 'avg_pnl', 'avg_pnl_pct']
        
        return daily_stats
    
    def get_exit_reason_analysis(self):
        """Analyze exit reasons"""
        if self.trades_df is None or len(self.trades_df) == 0:
            return None
        
        exit_analysis = self.trades_df.groupby('exit_reason').agg({
            'realized_pnl': ['count', 'sum', 'mean']
        }).round(2)
        
        exit_analysis.columns = ['count', 'total_pnl', 'avg_pnl']
        
        return exit_analysis
    
    def plot_equity_curve(self):
        """Generate equity curve from trades"""
        if self.trades_df is None or len(self.trades_df) == 0:
            print("⚠️  No trades to plot")
            return
        
        df = self.trades_df.sort_values('exit_time').copy()
        df['cumulative_pnl'] = df['realized_pnl'].cumsum()
        
        print("\n📈 EQUITY CURVE:")
        print(df[['exit_time', 'instrument_token', 'realized_pnl', 'cumulative_pnl']])
    
    def analyze_position_snapshots(self):
        """Analyze position history snapshots"""
        if self.positions_history_df is None or len(self.positions_history_df) == 0:
            print("⚠️  No position snapshots found")
            return None
        
        df = self.positions_history_df.copy()
        
        # Group by snapshot time to get portfolio value at each point
        portfolio_snapshots = df.groupby('snapshot_time').agg({
            'unrealized_pnl': 'sum',
            'instrument_token': 'count'
        }).rename(columns={'instrument_token': 'num_positions'})
        
        return portfolio_snapshots
    
    def export_analysis_report(self, output_file="strategy_analysis_report.xlsx"):
        """Export comprehensive analysis to Excel"""
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Performance summary
                if self.trades_df is not None:
                    summary = pd.DataFrame([self.get_performance_summary()])
                    summary.to_excel(writer, sheet_name='Summary', index=False)
                    
                    # All trades
                    self.trades_df.to_excel(writer, sheet_name='All Trades', index=False)
                    
                    # By instrument
                    inst_stats = self.get_trades_by_instrument()
                    if inst_stats is not None:
                        inst_stats.to_excel(writer, sheet_name='By Instrument')
                    
                    # By date
                    daily_stats = self.get_trades_by_date()
                    if daily_stats is not None:
                        daily_stats.to_excel(writer, sheet_name='Daily Performance')
                    
                    # Exit reasons
                    exit_stats = self.get_exit_reason_analysis()
                    if exit_stats is not None:
                        exit_stats.to_excel(writer, sheet_name='Exit Reasons')
                
                # All signals
                if self.signals_df is not None:
                    self.signals_df.to_excel(writer, sheet_name='All Signals', index=False)
                
                # Position snapshots
                if self.positions_history_df is not None:
                    self.positions_history_df.to_excel(writer, sheet_name='Position Snapshots', index=False)
            
            print(f"✅ Analysis report exported to: {output_file}")
        
        except Exception as e:
            print(f"❌ Error exporting report: {e}")


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    
    # Configuration
    config = StrategyConfig()
    
    # List of instruments to monitor
    # Replace with your actual instrument tokens
    INSTRUMENTS = [
        424961,
        # Add more instruments here
    ]
    
    # Date folder to monitor (today's date)
    DATE_FOLDER = datetime.now().strftime("%Y-%m-%d")
    
    # ========================================================================
    # MODE 1: RUN STRATEGY
    # ========================================================================
    
    print("="*80)
    print("SELECT MODE:")
    print("1. Run Strategy (single scan)")
    print("2. Run Strategy (continuous)")
    print("3. Analyze Historical Data")
    print("="*80)
    
    mode = 1  # Change this to switch modes
    
    if mode in [1, 2]:
        # Initialize strategy
        strategy = EMABreakoutStrategy(config, INSTRUMENTS)
        
        if mode == 1:
            # Single scan
            print("\n🔄 Running single scan...")
            strategy.run_single_scan(DATE_FOLDER)
        else:
            # Continuous monitoring
            print("\n♾️  Running continuous monitoring...")
            strategy.run_continuous(DATE_FOLDER)
        
        # Print session summary
        print("\n" + "="*80)
        print("📈 SESSION SUMMARY")
        print("="*80)
        print(f"Total Signals Generated: {len(strategy.signals_history)}")
        print(f"Completed Trades: {len(strategy.trades_history)}")
        print(f"Active Positions: {len(strategy.positions)}")
        
        if strategy.trades_history:
            trades_df = pd.DataFrame(strategy.trades_history)
            print(f"\n💰 Total Realized P&L: ₹{trades_df['realized_pnl'].sum():+.2f}")
            print(f"📊 Win Rate: {(trades_df['realized_pnl'] > 0).mean()*100:.1f}%")
            print(f"📈 Average P&L: ₹{trades_df['realized_pnl'].mean():+.2f}")
    
    # ========================================================================
    # MODE 2: ANALYZE HISTORICAL DATA
    # ========================================================================
    
    elif mode == 3:
        print("\n📊 Analyzing historical trading data...")
        
        analyzer = StrategyAnalyzer(output_dir=config.OUTPUT_DIR)
        
        # Print performance report
        analyzer.print_performance_report()
        
        # Show trades by instrument
        if analyzer.trades_df is not None:
            print("\n" + "="*80)
            print("📊 PERFORMANCE BY INSTRUMENT")
            print("="*80)
            inst_stats = analyzer.get_trades_by_instrument()
            print(inst_stats)
            
            print("\n" + "="*80)
            print("📅 DAILY PERFORMANCE")
            print("="*80)
            daily_stats = analyzer.get_trades_by_date()
            print(daily_stats)
            
            print("\n" + "="*80)
            print("🚪 EXIT REASON ANALYSIS")
            print("="*80)
            exit_stats = analyzer.get_exit_reason_analysis()
            print(exit_stats)
            
            # Show equity curve
            analyzer.plot_equity_curve()
        
        # Analyze position snapshots
        if analyzer.positions_history_df is not None:
            print("\n" + "="*80)
            print("📸 POSITION SNAPSHOT ANALYSIS")
            print("="*80)
            portfolio_snapshots = analyzer.analyze_position_snapshots()
            print(portfolio_snapshots.tail(20))  # Last 20 snapshots
        
        # Export to Excel
        print("\n" + "="*80)
        print("💾 EXPORTING ANALYSIS TO EXCEL")
        print("="*80)
        analyzer.export_analysis_report("strategy_analysis_report.xlsx")