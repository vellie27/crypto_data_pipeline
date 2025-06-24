import requests
import psycopg2
import json
from datetime import datetime

# --- Configuration ---
# Binance API Base URL
BINANCE_API_BASE_URL = "https://api.binance.com/api/v3"

# PostgreSQL Database Configuration
DB_HOST = "pg-209e851f-velyvineayieta-0621.l.aivencloud.com"
DB_NAME = "defaultdb"
DB_USER = "avnadmin"  # <--- REPLACE WITH YOUR POSTGRESQL USERNAME
DB_PASSWORD = "AVNS_awyu_iZtPBe8AJq6pjF"  # <--- REPLACE WITH YOUR POSTGRESQL PASSWORD

# Example Symbols for data fetching (you can extend this list)
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT"] # Added XRPUSDT and ADAUSDT

# Limit for order book and klines data (for demonstration)
ORDER_BOOK_LIMIT = 100
KLINES_LIMIT = 100 # Represents 100 candlesticks

# --- Database Connection and Setup ---
def get_db_connection():
    """Establishes and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Successfully connected to PostgreSQL database.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def create_tables(cur):
    """
    Creates necessary tables in the PostgreSQL database if they don't already exist.
    Adds proper indexing and primary keys for efficient querying.
    """
    print("Creating database tables if they do not exist...")

    # Table for Latest Prices
    cur.execute("""
        CREATE TABLE IF NOT EXISTS latest_prices (
            symbol VARCHAR(20) PRIMARY KEY,
            price NUMERIC(20, 10) NOT NULL,
            timestamp BIGINT NOT NULL,
            ingestion_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_latest_prices_symbol ON latest_prices (symbol);
        CREATE INDEX IF NOT EXISTS idx_latest_prices_timestamp ON latest_prices (timestamp);
    """)

    # Table for Order Book (Depth)
    # Bids and Asks are stored as JSONB for flexible storage of arrays of arrays
    cur.execute("""
        CREATE TABLE IF NOT EXISTS order_book (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            last_update_id BIGINT,
            bids JSONB NOT NULL,
            asks JSONB NOT NULL,
            timestamp BIGINT NOT NULL,
            ingestion_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_order_book_symbol ON order_book (symbol);
        CREATE INDEX IF NOT EXISTS idx_order_book_timestamp ON order_book (timestamp);
    """)

    # Table for Recent Trades
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recent_trades (
            trade_id BIGINT PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            price NUMERIC(20, 10) NOT NULL,
            qty NUMERIC(20, 10) NOT NULL,
            quote_qty NUMERIC(20, 10) NOT NULL,
            trade_time BIGINT NOT NULL,
            is_buyer_maker BOOLEAN NOT NULL,
            is_best_match BOOLEAN NOT NULL,
            ingestion_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_recent_trades_symbol ON recent_trades (symbol);
        CREATE INDEX IF NOT EXISTS idx_recent_trades_time ON recent_trades (trade_time);
    """)

    # Table for Klines (Candlesticks)
    # Using a composite primary key for symbol and open_time
    cur.execute("""
        CREATE TABLE IF NOT EXISTS klines (
            symbol VARCHAR(20) NOT NULL,
            open_time BIGINT NOT NULL,
            open_price NUMERIC(20, 10) NOT NULL,
            high_price NUMERIC(20, 10) NOT NULL,
            low_price NUMERIC(20, 10) NOT NULL,
            close_price NUMERIC(20, 10) NOT NULL,
            volume NUMERIC(20, 10) NOT NULL,
            close_time BIGINT NOT NULL,
            quote_asset_volume NUMERIC(20, 10) NOT NULL,
            number_of_trades BIGINT NOT NULL,
            taker_buy_base_asset_volume NUMERIC(20, 10) NOT NULL,
            taker_buy_quote_asset_volume NUMERIC(20, 10) NOT NULL,
            ignore_field NUMERIC(20, 10),
            ingestion_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, open_time)
        );
        CREATE INDEX IF NOT EXISTS idx_klines_symbol ON klines (symbol);
        CREATE INDEX IF NOT EXISTS idx_klines_open_time ON klines (open_time);
    """)

    # Table for 24hr Ticker Stats
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ticker_24hr (
            symbol VARCHAR(20) PRIMARY KEY,
            price_change NUMERIC(20, 10),
            price_change_percent NUMERIC(20, 10),
            weighted_avg_price NUMERIC(20, 10),
            prev_close_price NUMERIC(20, 10),
            last_price NUMERIC(20, 10) NOT NULL,
            last_qty NUMERIC(20, 10),
            bid_price NUMERIC(20, 10),
            bid_qty NUMERIC(20, 10),
            ask_price NUMERIC(20, 10),
            ask_qty NUMERIC(20, 10),
            open_price NUMERIC(20, 10),
            high_price NUMERIC(20, 10),
            low_price NUMERIC(20, 10),
            volume NUMERIC(20, 10),
            quote_volume NUMERIC(20, 10),
            open_time BIGINT,
            close_time BIGINT,
            first_id BIGINT,
            last_id BIGINT,
            count BIGINT,
            ingestion_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_ticker_24hr_symbol ON ticker_24hr (symbol);
    """)
    print("Tables creation complete.")


# --- Binance API Data Fetching ---
def fetch_binance_data(endpoint, params=None):
    """
    Fetches data from a specified Binance API endpoint.

    Args:
        endpoint (str): The API endpoint (e.g., "/api/v3/ticker/price").
        params (dict, optional): Dictionary of query parameters. Defaults to None.

    Returns:
        dict or list: The JSON response from the API, or None if an error occurs.
    """
    url = f"{BINANCE_API_BASE_URL}{endpoint}"
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

# --- Data Ingestion Functions ---
def ingest_latest_prices(cur, data):
    """
    Ingests latest prices data into the latest_prices table.
    Uses ON CONFLICT DO UPDATE for symbols that already exist.
    """
    print(f"Ingesting latest prices for {len(data)} symbols...")
    for item in data:
        symbol = item.get("symbol")
        price = float(item.get("price"))
        timestamp = datetime.now().timestamp() * 1000 # Use current time as Binance /ticker/price doesn't provide it directly

        cur.execute("""
            INSERT INTO latest_prices (symbol, price, timestamp)
            VALUES (%s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE
            SET price = EXCLUDED.price,
                timestamp = EXCLUDED.timestamp,
                ingestion_time = CURRENT_TIMESTAMP;
        """, (symbol, price, timestamp))
    print("Latest prices ingestion complete.")

def ingest_order_book(cur, symbol, data):
    """Ingests order book data into the order_book table."""
    print(f"Ingesting order book for {symbol}...")
    last_update_id = data.get("lastUpdateId")
    bids = json.dumps(data.get("bids"))  # Convert list of lists to JSON string
    asks = json.dumps(data.get("asks"))  # Convert list of lists to JSON string
    timestamp = datetime.now().timestamp() * 1000 # Use current time

    cur.execute("""
        INSERT INTO order_book (symbol, last_update_id, bids, asks, timestamp)
        VALUES (%s, %s, %s, %s, %s);
    """, (symbol, last_update_id, bids, asks, timestamp))
    print(f"Order book for {symbol} ingestion complete.")


def ingest_recent_trades(cur, symbol, data):
    """Ingests recent trades data into the recent_trades table."""
    print(f"Ingesting recent trades for {symbol}...")
    for trade in data:
        trade_id = trade.get("id")
        price = float(trade.get("price"))
        qty = float(trade.get("qty"))
        quote_qty = float(trade.get("quoteQty"))
        trade_time = trade.get("time")
        is_buyer_maker = trade.get("isBuyerMaker")
        is_best_match = trade.get("isBestMatch")

        # Use ON CONFLICT DO NOTHING to avoid inserting duplicate trades
        cur.execute("""
            INSERT INTO recent_trades (trade_id, symbol, price, qty, quote_qty, trade_time, is_buyer_maker, is_best_match)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_id) DO NOTHING;
        """, (trade_id, symbol, price, qty, quote_qty, trade_time, is_buyer_maker, is_best_match))
    print(f"Recent trades for {symbol} ingestion complete.")


def ingest_klines(cur, symbol, interval, data):
    """
    Ingests klines (candlestick) data into the klines table.
    Uses ON CONFLICT DO UPDATE for existing klines.
    """
    print(f"Ingesting klines for {symbol} with interval {interval}...")
    for kline in data:
        # Binance klines format:
        # [
        #   [
        #     1499040000000,      // Open time
        #     "0.01634790",       // Open
        #     "0.80000000",       // High
        #     "0.01575600",       // Low
        #     "0.01577100",       // Close
        #     "148976.10700000",  // Volume
        #     1499644799999,      // Close time
        #     "2434.19231072",    // Quote asset volume
        #     308,                // Number of trades
        #     "1756.87400000",    // Taker buy base asset volume
        #     "28.46694368",      // Taker buy quote asset volume
        #     "17928899.62484339" // Ignore
        #   ]
        # ]
        open_time = kline[0]
        open_price = float(kline[1])
        high_price = float(kline[2])
        low_price = float(kline[3])
        close_price = float(kline[4])
        volume = float(kline[5])
        close_time = kline[6]
        quote_asset_volume = float(kline[7])
        number_of_trades = kline[8]
        taker_buy_base_asset_volume = float(kline[9])
        taker_buy_quote_asset_volume = float(kline[10])
        ignore_field = float(kline[11]) if kline[11] else None # Can be an empty string in some cases

        cur.execute("""
            INSERT INTO klines (
                symbol, open_time, open_price, high_price, low_price, close_price,
                volume, close_time, quote_asset_volume, number_of_trades,
                taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore_field
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, open_time) DO UPDATE
            SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                close_time = EXCLUDED.close_time,
                quote_asset_volume = EXCLUDED.quote_asset_volume,
                number_of_trades = EXCLUDED.number_of_trades,
                taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
                taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
                ignore_field = EXCLUDED.ignore_field,
                ingestion_time = CURRENT_TIMESTAMP;
        """, (symbol, open_time, open_price, high_price, low_price, close_price,
              volume, close_time, quote_asset_volume, number_of_trades,
              taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore_field))
    print(f"Klines for {symbol} ingestion complete.")


def ingest_ticker_24hr(cur, data):
    """
    Ingests 24hr ticker stats into the ticker_24hr table.
    Uses ON CONFLICT DO UPDATE.
    """
    print(f"Ingesting 24hr ticker stats for {len(data)} symbols...")
    for item in data:
        symbol = item.get("symbol")
        price_change = float(item.get("priceChange"))
        price_change_percent = float(item.get("priceChangePercent"))
        weighted_avg_price = float(item.get("weightedAvgPrice"))
        prev_close_price = float(item.get("prevClosePrice"))
        last_price = float(item.get("lastPrice"))
        last_qty = float(item.get("lastQty"))
        bid_price = float(item.get("bidPrice"))
        bid_qty = float(item.get("bidQty"))
        ask_price = float(item.get("askPrice"))
        ask_qty = float(item.get("askQty"))
        open_price = float(item.get("openPrice"))
        high_price = float(item.get("highPrice"))
        low_price = float(item.get("lowPrice"))
        volume = float(item.get("volume"))
        quote_volume = float(item.get("quoteVolume"))
        open_time = item.get("openTime")
        close_time = item.get("closeTime")
        first_id = item.get("firstId")
        last_id = item.get("lastId")
        count = item.get("count")

        cur.execute("""
            INSERT INTO ticker_24hr (
                symbol, price_change, price_change_percent, weighted_avg_price,
                prev_close_price, last_price, last_qty, bid_price, bid_qty,
                ask_price, ask_qty, open_price, high_price, low_price,
                volume, quote_volume, open_time, close_time, first_id, last_id, count
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (symbol) DO UPDATE
            SET
                price_change = EXCLUDED.price_change,
                price_change_percent = EXCLUDED.price_change_percent,
                weighted_avg_price = EXCLUDED.weighted_avg_price,
                prev_close_price = EXCLUDED.prev_close_price,
                last_price = EXCLUDED.last_price,
                last_qty = EXCLUDED.last_qty,
                bid_price = EXCLUDED.bid_price,
                bid_qty = EXCLUDED.bid_qty,
                ask_price = EXCLUDED.ask_price,
                ask_qty = EXCLUDED.ask_qty,
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                open_time = EXCLUDED.open_time,
                close_time = EXCLUDED.close_time,
                first_id = EXCLUDED.first_id,
                last_id = EXCLUDED.last_id,
                count = EXCLUDED.count,
                ingestion_time = CURRENT_TIMESTAMP;
        """, (
            symbol, price_change, price_change_percent, weighted_avg_price,
            prev_close_price, last_price, last_qty, bid_price, bid_qty,
            ask_price, ask_qty, open_price, high_price, low_price,
            volume, quote_volume, open_time, close_time, first_id, last_id, count
        ))
    print("24hr Ticker stats ingestion complete.")


def main():
    """Main function to orchestrate data fetching and ingestion."""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return

        cur = conn.cursor()
        create_tables(cur)

        # 1. Fetch and ingest Latest Prices (all symbols)
        print("\n--- Fetching Latest Prices ---")
        latest_prices_data = fetch_binance_data("/api/v3/ticker/price")
        if latest_prices_data:
            ingest_latest_prices(cur, latest_prices_data)
            conn.commit()

        for symbol in SYMBOLS:
            print(f"\n--- Fetching data for {symbol} ---")

            # 2. Fetch and ingest Order Book (Depth)
            print("Fetching Order Book...")
            order_book_data = fetch_binance_data("/api/v3/depth", params={"symbol": symbol, "limit": ORDER_BOOK_LIMIT})
            if order_book_data:
                ingest_order_book(cur, symbol, order_book_data)
                conn.commit()

            # 3. Fetch and ingest Recent Trades
            print("Fetching Recent Trades...")
            recent_trades_data = fetch_binance_data("/api/v3/trades", params={"symbol": symbol, "limit": 100}) # Max 1000
            if recent_trades_data:
                ingest_recent_trades(cur, symbol, recent_trades_data)
                conn.commit()

            # 4. Fetch and ingest Klines (Candlesticks) - Example: 1-hour interval
            # Interval can be: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
            print("Fetching Klines (1-hour interval)...")
            klines_data = fetch_binance_data("/api/v3/klines", params={"symbol": symbol, "interval": "1h", "limit": KLINES_LIMIT})
            if klines_data:
                ingest_klines(cur, symbol, "1h", klines_data)
                conn.commit()

            # 5. Fetch and ingest 24hr Ticker Stats (for individual symbol)
            # Binance API returns a list for /api/v3/ticker/24hr without symbol param
            # and a single object if symbol param is provided.
            print("Fetching 24hr Ticker Stats...")
            ticker_24hr_data = fetch_binance_data("/api/v3/ticker/24hr", params={"symbol": symbol})
            if ticker_24hr_data:
                # API returns a single dictionary for specific symbol, put it in a list for ingest function
                ingest_ticker_24hr(cur, [ticker_24hr_data])
                conn.commit()
            
        print("\nData fetching and ingestion complete for all specified endpoints and symbols.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        if conn:
            conn.rollback() # Rollback any pending transactions on error
    finally:
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")

if __name__ == "__main__":
    main()
