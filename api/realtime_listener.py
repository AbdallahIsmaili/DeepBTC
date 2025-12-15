# maining/realtime_listener.py

from datetime import datetime
import websocket
import json
import time
import sys

# --- Configuration ---
# Public WebSocket for Binance Spot Trades (unauthenticated)
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# --- Callback Functions ---
def on_message(ws, message):
    """
    Called every time a new trade message is received.
    This is where you integrate your trained ML model for live prediction.
    """
    try:
        data = json.loads(message)
        
        # We are interested in Aggregated Trade data fields:
        # 'p': Price, 'q': Quantity, 'T': Trade Time
        
        trade_price = data.get('p')
        trade_quantity = data.get('q')
        
        if trade_price:
            timestamp = datetime.fromtimestamp(data.get('T') / 1000.0)
            
            # --- LIVE DATA INGESTION POINT ---
            # In a full project, you would process this data:
            # 1. Update your local order book (if streaming depth).
            # 2. Calculate real-time features (e.g., fast volume moving averages).
            # 3. Feed features to your trained ML model to get a trading signal.
            # ---------------------------------
            
            print(f"[{timestamp.strftime('%H:%M:%S.%f')[:-3]}] LIVE TRADE: Price={trade_price:<10} Qty={trade_quantity:<10}")

    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"### ERROR ###: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### CONNECTION CLOSED ###")
    print(f"Status Code: {close_status_code}, Message: {close_msg}")
    
def on_open(ws):
    print(f"✅ Connected to Binance WebSocket at {BINANCE_WS_URL}. Streaming live BTC trades...")

def start_websocket_stream():
    """Initializes and runs the WebSocket connection."""
    
    # WebSocketApp setup
    ws_app = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    # Note: run_forever() is blocking and needs error handling for reconnection in production.
    # For a simple script, this is sufficient.
    print("Starting stream (Press Ctrl+C to stop)...")
    ws_app.run_forever()

if __name__ == '__main__':
    start_websocket_stream()