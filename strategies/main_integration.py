# main_integration.py
import logging
from ZerodhaKiteDataDisplay import ZerodhaKiteDataDisplay
from RealTimeOrderFlowStrategy import RealTimeOrderFlowStrategy
import time
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_data_stream():
    """
    Create and initialize the DataLiveStream object
    """
    try:
        print("🚀 Initializing ICICI Direct Breeze Data Stream...")
        
        # Create data stream instance (it will auto-initialize with environment variables)
        data_stream = ZerodhaKiteDataDisplay()
        
        print("✅ Data stream created successfully!")
        return data_stream
        
    except Exception as e:
        logger.error(f"❌ Failed to create data stream: {e}")
        raise

def setup_strategies(data_stream, symbols):
    """
    Setup and register OrderFlow strategies for multiple symbols
    
    Args:
        data_stream: ICICIBreezeDataDisplay instance
        symbols: List of symbols to monitor
    """
    strategies = {}
    
    for symbol in symbols:
        try:
            print(f"📈 Setting up OrderFlow strategy for {symbol}...")
            
            # Create strategy instance
            strategy = RealTimeOrderFlowStrategy(
                symbol=symbol,
                lookback_period=100,
                initial_cash=100000
            )
            
            # Register strategy with data stream
            strategy.register_with_datastream(data_stream)
            
            strategies[symbol] = strategy
            print(f"✅ Strategy setup completed for {symbol}")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup strategy for {symbol}: {e}")
    
    return strategies

def monitor_strategies(strategies):
    """
    Monitor and display strategy performance
    
    Args:
        strategies: Dictionary of strategy instances
    """
    print("\n" + "="*60)
    print("📊 STRATEGY MONITORING")
    print("="*60)
    
    for symbol, strategy in strategies.items():
        status = strategy.get_strategy_status()
        performance = strategy.get_performance_summary()
        
        print(f"\n📈 {symbol}:")
        print(f"   Position: {status['position']}")
        print(f"   OHLCV Records: {status['ohlcv_records']}")
        print(f"   Tick Records: {status['tick_records']}")
        print(f"   Total Signals: {performance['total_signals']}")
        
        # Show recent signals
        if performance['recent_signals']:
            latest_signal = performance['recent_signals'][-1]
            print(f"   Latest Signal: {latest_signal['direction']} "
                  f"(Confidence: {latest_signal['confidence']:.2f})")

def main():
    """
    Main function to run the complete trading system
    """
    try:
        # Symbols to monitor
        #symbols = ["RELIANCE", "TCS", "INFY"]
        symbols = [738561, 2953217]
        #example_tokens = [738561, 2953217]
        print("="*70)
        print("🤖 AI TRADING SYSTEM - ORDER FLOW STRATEGY")
        print("="*70)
        
        # Step 1: Create data stream
        data_stream = create_data_stream()
        
        # Step 2: Setup strategies
        strategies = setup_strategies(data_stream, symbols)
        
        if not strategies:
            print("❌ No strategies were successfully setup. Exiting.")
            return
        
        print(f"\n✅ Successfully setup {len(strategies)} strategies")
        
        # Step 3: Start data streaming
        print("\n🎯 Starting real-time data streaming...")
        print("   Press Ctrl+C to stop\n")
        
        # We'll run the streaming in a separate thread to allow monitoring
        import threading
        
        def start_streaming():
            data_stream.start_streaming(symbols)
        
        # Start streaming in background thread
        stream_thread = threading.Thread(target=start_streaming, daemon=True)
        stream_thread.start()
        
        # Main monitoring loop
        try:
            while True:
                #monitor_strategies(strategies)
                print(f"\n⏰ Last update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("🔄 Refreshing in 30 seconds...")
                time.sleep(30)
                print("\n" + "="*60)
                
        except KeyboardInterrupt:
            print("\n\n🛑 Shutting down trading system...")
            
            # Display final summary
            print("\n📋 FINAL STRATEGY SUMMARY")
            print("="*50)
            for symbol, strategy in strategies.items():
                performance = strategy.get_performance_summary()
                print(f"\n{symbol}:")
                print(f"  Total Signals: {performance['total_signals']}")
                print(f"  Current Position: {performance['current_position']}")
                
            print("\n🎯 Trading system stopped successfully!")
            
    except Exception as e:
        logger.error(f"❌ Critical error in main execution: {e}")

# Alternative: Simple setup for single symbol
def simple_example():
    """
    Simple example for quick testing
    """
    try:
        # Create data stream
        data_stream = ZerodhaKiteDataDisplay()
        
        # Create strategy for single symbol
        strategy = RealTimeOrderFlowStrategy("RELIANCE")
        
        # Register strategy
        strategy.register_with_datastream(data_stream)
        
        print("✅ Simple setup completed!")
        print("   Starting data stream for RELIANCE...")
        
        # Start streaming (this will block)
        data_stream.start_streaming(["RELIANCE"])
        
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    except Exception as e:
        logger.error(f"Error in simple example: {e}")

# Demonstration of callback functionality
def demonstration_mode():
    """
    Run in demonstration mode with sample callbacks
    """
    def demo_ohlc_handler(ohlc_data):
        symbol = ohlc_data.get('stock_code', 'Unknown')
        price = ohlc_data.get('close', 0)
        print(f"📊 DEMO: {symbol} OHLC - Price: ₹{price:.2f}")
    
    def demo_tick_handler(tick_data):
        symbol = tick_data.get('stock_code', 'Unknown')
        price = tick_data.get('last', 0)
        print(f"📊 DEMO: {symbol} Tick - Price: ₹{price:.2f}")
    
    try:
        data_stream = ZerodhaKiteDataDisplay()
        
        # Register demo callbacks
        data_stream.register_ohlc_callback(demo_ohlc_handler)
        data_stream.register_tick_callback(demo_tick_handler)
        
        print("🎮 Running in demonstration mode...")
        data_stream.start_streaming(["RELIANCE", "TCS"])
        
    except KeyboardInterrupt:
        print("\n🎮 Demonstration stopped")
    except Exception as e:
        logger.error(f"Demonstration error: {e}")

if __name__ == "__main__":
    # Run the complete system
    main()
    
    # Or run simple example
    # simple_example()
    
    # Or run demonstration
    # demonstration_mode()