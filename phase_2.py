import psycopg2
from cassandra.cluster import Cluster
from cassandra.io.asyncio import AsyncioEventLoop
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cdc_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
class Config:
    # PostgreSQL Configuration
    PG_HOST = os.getenv("PG_HOST", "localhost")
    PG_DATABASE = os.getenv("PG_DATABASE", "defaultdb")
    PG_USER = os.getenv("PG_USER", "postgres")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "")
    
    # Cassandra Configuration
    CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
    CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "binance_time_series")
    CASSANDRA_USER = os.getenv("CASSANDRA_USER", "")
    CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "")
    
    # CDC Configuration
    CDC_CHECK_INTERVAL = int(os.getenv("CDC_CHECK_INTERVAL", "10"))
    CDC_LOOKBACK = int(os.getenv("CDC_LOOKBACK", "60"))

class DatabaseConnector:
    @staticmethod
    def get_postgres_connection():
        """Establish PostgreSQL connection."""
        try:
            conn = psycopg2.connect(
                host=Config.PG_HOST,
                database=Config.PG_DATABASE,
                user=Config.PG_USER,
                password=Config.PG_PASSWORD
            )
            logger.info("Successfully connected to PostgreSQL")
            return conn
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL connection error: {e}")
            return None

    @staticmethod
    def get_cassandra_session():
        """Establish Cassandra session."""
        try:
            auth_provider = None
            if Config.CASSANDRA_USER and Config.CASSANDRA_PASSWORD:
                auth_provider = PlainTextAuthProvider(
                    username=Config.CASSANDRA_USER,
                    password=Config.CASSANDRA_PASSWORD
                )

            cluster = Cluster(
                contact_points=Config.CASSANDRA_HOSTS,
                port=Config.CASSANDRA_PORT,
                auth_provider=auth_provider,
                event_loop_class=AsyncioEventLoop
            )
            session = cluster.connect()

            # Create keyspace if not exists
            session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {Config.CASSANDRA_KEYSPACE}
                WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};
            """)
            session.set_keyspace(Config.CASSANDRA_KEYSPACE)
            logger.info(f"Cassandra keyspace '{Config.CASSANDRA_KEYSPACE}' ensured")
            return session
        except Exception as e:
            logger.error(f"Cassandra connection error: {e}")
            return None

class TableManager:
    @staticmethod
    def create_cassandra_tables(session):
        """Create required Cassandra tables if they don't exist."""
        tables = {
            'latest_prices_cassandra': """
                CREATE TABLE IF NOT EXISTS latest_prices_cassandra (
                    symbol text,
                    price decimal,
                    timestamp bigint,
                    ingestion_time timestamp,
                    PRIMARY KEY (symbol)
                );""",
            'order_book_cassandra': """
                CREATE TABLE IF NOT EXISTS order_book_cassandra (
                    symbol text,
                    timestamp bigint,
                    last_update_id bigint,
                    bids text,
                    asks text,
                    ingestion_time timestamp,
                    PRIMARY KEY ((symbol), timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC);""",
            'recent_trades_cassandra': """
                CREATE TABLE IF NOT EXISTS recent_trades_cassandra (
                    symbol text,
                    trade_time bigint,
                    trade_id bigint,
                    price decimal,
                    qty decimal,
                    quote_qty decimal,
                    is_buyer_maker boolean,
                    is_best_match boolean,
                    ingestion_time timestamp,
                    PRIMARY KEY ((symbol), trade_time, trade_id)
                ) WITH CLUSTERING ORDER BY (trade_time DESC);""",
            'klines_cassandra': """
                CREATE TABLE IF NOT EXISTS klines_cassandra (
                    symbol text,
                    open_time bigint,
                    close_time bigint,
                    open_price decimal,
                    high_price decimal,
                    low_price decimal,
                    close_price decimal,
                    volume decimal,
                    quote_asset_volume decimal,
                    number_of_trades bigint,
                    taker_buy_base_asset_volume decimal,
                    taker_buy_quote_asset_volume decimal,
                    ignore_field decimal,
                    ingestion_time timestamp,
                    PRIMARY KEY ((symbol), open_time)
                ) WITH CLUSTERING ORDER BY (open_time DESC);""",
            'ticker_24hr_cassandra': """
                CREATE TABLE IF NOT EXISTS ticker_24hr_cassandra (
                    symbol text,
                    ingestion_time timestamp,
                    price_change decimal,
                    price_change_percent decimal,
                    weighted_avg_price decimal,
                    prev_close_price decimal,
                    last_price decimal,
                    last_qty decimal,
                    bid_price decimal,
                    bid_qty decimal,
                    ask_price decimal,
                    ask_qty decimal,
                    open_price decimal,
                    high_price decimal,
                    low_price decimal,
                    volume decimal,
                    quote_volume decimal,
                    open_time bigint,
                    close_time bigint,
                    first_id bigint,
                    last_id bigint,
                    count bigint,
                    PRIMARY KEY ((symbol), ingestion_time)
                ) WITH CLUSTERING ORDER BY (ingestion_time DESC);"""
        }

        try:
            for table_name, query in tables.items():
                session.execute(query)
                logger.info(f"Ensured table exists: {table_name}")
        except Exception as e:
            logger.error(f"Error creating Cassandra tables: {e}")

class DataReplicator:
    @staticmethod
    def replicate_data(pg_cur, c_session, table_name, query, fields, last_checked_time):
        """Generic method to replicate data from PostgreSQL to Cassandra."""
        try:
            pg_cur.execute(query, (last_checked_time,))
            rows = pg_cur.fetchall()
            
            if not rows:
                logger.debug(f"No new data found for {table_name}")
                return 0
                
            logger.info(f"Found {len(rows)} new records for {table_name}")
            
            for row in rows:
                try:
                    c_session.execute(
                        f"INSERT INTO {table_name} ({','.join(fields.keys())}) VALUES ({','.join(['%s']*len(fields))})",
                        [fields[col](val) if val is not None else None for col, val in zip(fields.keys(), row)]
                    )
                except Exception as e:
                    logger.error(f"Error replicating record to {table_name}: {e}")
            
            return len(rows)
        except Exception as e:
            logger.error(f"Error during {table_name} replication: {e}")
            return 0

class CDCPipeline:
    def __init__(self):
        self.pg_conn = None
        self.c_session = None
        self.last_run_times = {
            'latest_prices': datetime.min,
            'order_book': datetime.min,
            'recent_trades': datetime.min,
            'klines': datetime.min,
            'ticker_24hr': datetime.min,
        }

    def setup(self):
        """Initialize database connections."""
        self.pg_conn = DatabaseConnector.get_postgres_connection()
        if not self.pg_conn:
            return False

        self.c_session = DatabaseConnector.get_cassandra_session()
        if not self.c_session:
            self.pg_conn.close()
            return False

        TableManager.create_cassandra_tables(self.c_session)
        return True

    def run_replication_cycle(self):
        """Execute one full replication cycle."""
        current_check_time = datetime.utcnow()
        cdc_start_time = current_check_time - timedelta(seconds=Config.CDC_LOOKBACK)
        
        logger.info(f"CDC cycle started at {current_check_time.isoformat()}")
        
        with self.pg_conn.cursor() as pg_cur:
            # Define replication tasks
            tasks = [
                {
                    'table': 'latest_prices_cassandra',
                    'query': """SELECT symbol, price, timestamp, ingestion_time 
                                FROM latest_prices WHERE ingestion_time > %s""",
                    'fields': {
                        'symbol': str,
                        'price': float,
                        'timestamp': int,
                        'ingestion_time': str
                    }
                },
                {
                    'table': 'order_book_cassandra',
                    'query': """SELECT symbol, timestamp, last_update_id, bids, asks, ingestion_time 
                                FROM order_book WHERE ingestion_time > %s""",
                    'fields': {
                        'symbol': str,
                        'timestamp': int,
                        'last_update_id': int,
                        'bids': str,
                        'asks': str,
                        'ingestion_time': str
                    }
                },
                {
                    'table': 'recent_trades_cassandra',
                    'query': """SELECT symbol, trade_time, trade_id, price, qty, quote_qty, 
                                is_buyer_maker, is_best_match, ingestion_time 
                                FROM recent_trades WHERE ingestion_time > %s""",
                    'fields': {
                        'symbol': str,
                        'trade_time': int,
                        'trade_id': int,
                        'price': float,
                        'qty': float,
                        'quote_qty': float,
                        'is_buyer_maker': bool,
                        'is_best_match': bool,
                        'ingestion_time': str
                    }
                },
                {
                    'table': 'klines_cassandra',
                    'query': """SELECT symbol, open_time, close_time, open_price, high_price, low_price,
                                close_price, volume, quote_asset_volume, number_of_trades,
                                taker_buy_base_asset_volume, taker_buy_quote_asset_volume, 
                                ignore_field, ingestion_time FROM klines WHERE ingestion_time > %s""",
                    'fields': {
                        'symbol': str,
                        'open_time': int,
                        'close_time': int,
                        'open_price': float,
                        'high_price': float,
                        'low_price': float,
                        'close_price': float,
                        'volume': float,
                        'quote_asset_volume': float,
                        'number_of_trades': int,
                        'taker_buy_base_asset_volume': float,
                        'taker_buy_quote_asset_volume': float,
                        'ignore_field': float,
                        'ingestion_time': str
                    }
                },
                {
                    'table': 'ticker_24hr_cassandra',
                    'query': """SELECT symbol, ingestion_time, price_change, price_change_percent,
                                weighted_avg_price, prev_close_price, last_price, last_qty,
                                bid_price, bid_qty, ask_price, ask_qty, open_price, high_price,
                                low_price, volume, quote_volume, open_time, close_time,
                                first_id, last_id, count FROM ticker_24hr WHERE ingestion_time > %s""",
                    'fields': {
                        'symbol': str,
                        'ingestion_time': str,
                        'price_change': float,
                        'price_change_percent': float,
                        'weighted_avg_price': float,
                        'prev_close_price': float,
                        'last_price': float,
                        'last_qty': float,
                        'bid_price': float,
                        'bid_qty': float,
                        'ask_price': float,
                        'ask_qty': float,
                        'open_price': float,
                        'high_price': float,
                        'low_price': float,
                        'volume': float,
                        'quote_volume': float,
                        'open_time': int,
                        'close_time': int,
                        'first_id': int,
                        'last_id': int,
                        'count': int
                    }
                }
            ]

            # Execute all replication tasks
            for task in tasks:
                table_name = task['table']
                logger.info(f"Checking for {table_name} updates since {self.last_run_times[table_name.split('_')[0]]}")
                
                count = DataReplicator.replicate_data(
                    pg_cur=pg_cur,
                    c_session=self.c_session,
                    table_name=task['table'],
                    query=task['query'],
                    fields=task['fields'],
                    last_checked_time=self.last_run_times[table_name.split('_')[0]]
                )
                
                if count > 0:
                    self.last_run_times[table_name.split('_')[0]] = cdc_start_time

    def run(self):
        """Main pipeline execution."""
        try:
            if not self.setup():
                return

            logger.info("CDC pipeline started. Press Ctrl+C to stop.")
            
            while True:
                try:
                    self.run_replication_cycle()
                    logger.info(f"Waiting {Config.CDC_CHECK_INTERVAL} seconds for next cycle...")
                    time.sleep(Config.CDC_CHECK_INTERVAL)
                except Exception as e:
                    logger.error(f"Error during CDC cycle: {e}")
                    time.sleep(Config.CDC_CHECK_INTERVAL)  # Wait before retrying

        except KeyboardInterrupt:
            logger.info("CDC pipeline stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in CDC pipeline: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources."""
        if self.pg_conn:
            self.pg_conn.close()
            logger.info("PostgreSQL connection closed")
        if self.c_session:
            self.c_session.shutdown()
            logger.info("Cassandra session closed")

if __name__ == "__main__":
    pipeline = CDCPipeline()
    pipeline.run()
