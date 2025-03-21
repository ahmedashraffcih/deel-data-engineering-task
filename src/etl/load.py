from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_batch
from models.schema import OrderData, OrderItemData
from utils.database import get_source_connection
from utils.logger import logger
from config.settings import settings



class DataLoader:
    """Class to load data into the analytical database."""

    def __init__(self):
        self.conn = None

    def connect(self):
        """Connect to the database."""
        if self.conn is None or self.conn.closed:
            self.conn = get_source_connection()
        return self.conn

    def close(self):
        """Close the connection to the target database."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Connection to target database closed")

    def ensure_schema_exists(self):
        """Ensure the analytics schema exists."""
        conn = self.connect()

        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS analytics")

            # Create analytical orders table
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS analytics.analytical_orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                customer_name VARCHAR(100) NOT NULL,
                order_date TIMESTAMP NOT NULL,
                delivery_date TIMESTAMP,
                status VARCHAR(20) NOT NULL,
                total_items INTEGER NOT NULL,
                total_amount NUMERIC(10, 2) NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                created_by VARCHAR(20),
                updated_by VARCHAR(20)
            )
            """
            )

            # Create analytical order items table
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS analytics.analytical_order_items (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                product_name VARCHAR(100) NOT NULL,
                quantity INTEGER NOT NULL,
                price NUMERIC(10, 2) NOT NULL,
                order_status VARCHAR(20) NOT NULL,
                delivery_date TIMESTAMP,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                created_by VARCHAR(20),
                updated_by VARCHAR(20)
            )
            """
            )

            # Create indexes for optimized queries
            cursor.execute(
                """
            CREATE INDEX IF NOT EXISTS idx_orders_status ON analytics.analytical_orders (status);
            CREATE INDEX IF NOT EXISTS idx_orders_delivery_date ON analytics.analytical_orders (delivery_date);
            CREATE INDEX IF NOT EXISTS idx_items_product_id ON analytics.analytical_order_items (product_id);
            CREATE INDEX IF NOT EXISTS idx_items_order_status ON analytics.analytical_order_items (order_status);
            """
            )

        conn.commit()
        logger.info("Analytics schema and tables created or confirmed")

    def load_orders(self, orders: List[OrderData]):
        """Load orders into the analytical database."""
        if not orders:
            return

        conn = self.connect()

        upsert_query = """
        INSERT INTO analytics.analytical_orders (
            order_id, customer_id, customer_name, order_date, delivery_date, 
            status, total_items, total_amount, created_at, updated_at,
            created_by, updated_by
        ) VALUES (
            %(order_id)s, %(customer_id)s, %(customer_name)s, %(order_date)s, %(delivery_date)s,
            %(status)s, %(total_items)s, %(total_amount)s, %(created_at)s, %(updated_at)s,
            %(created_by)s, %(updated_by)s
        ) ON CONFLICT (order_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            customer_name = EXCLUDED.customer_name,
            order_date = EXCLUDED.order_date,
            delivery_date = EXCLUDED.delivery_date,
            status = EXCLUDED.status,
            total_items = EXCLUDED.total_items,
            total_amount = EXCLUDED.total_amount,
            updated_at = EXCLUDED.updated_at,
            updated_by = EXCLUDED.updated_by
        """

        order_dicts = [
            {
                "order_id": order.order_id,
                "customer_id": order.customer_id,
                "customer_name": order.customer_name,
                "order_date": order.order_date,
                "delivery_date": order.delivery_date,
                "status": order.status,
                "total_items": order.total_items,
                "total_amount": order.total_amount,
                "created_at": order.created_at,
                "updated_at": order.updated_at,
                "created_by": int(order.created_by) if order.created_by.isdigit() else -1,
                "updated_by": int(order.updated_by) if order.updated_by.isdigit() else -1,
            }
            for order in orders
        ]

        try:
            with conn.cursor() as cursor:
                execute_batch(
                    cursor, upsert_query, order_dicts, page_size=settings.BATCH_SIZE
                )
            conn.commit()
            logger.info(f"Loaded {len(orders)} orders into the analytical database")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading orders: {e}")
            logger.error(f"First order dict for reference: {order_dicts[0] if order_dicts else 'No orders'}")
            raise

    def load_order_items(self, order_items: List[OrderItemData]):
        """Load order items into the analytical database."""
        if not order_items:
            return

        conn = self.connect()

        upsert_query = """
        INSERT INTO analytics.analytical_order_items (
            id, order_id, product_id, product_name, quantity, 
            price, order_status, delivery_date, created_at, updated_at,
            created_by, updated_by
        ) VALUES (
            %(order_item_id)s, %(order_id)s, %(product_id)s, %(product_name)s, 
            %(quantity)s, %(price)s, %(order_status)s, %(delivery_date)s,
            %(created_at)s, %(updated_at)s, %(created_by)s, %(updated_by)s
        ) ON CONFLICT (id) DO UPDATE SET
            order_id = EXCLUDED.order_id,
            product_id = EXCLUDED.product_id,
            product_name = EXCLUDED.product_name,
            quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            order_status = EXCLUDED.order_status,
            delivery_date = EXCLUDED.delivery_date,
            updated_at = EXCLUDED.updated_at,
            created_by = EXCLUDED.created_by,
            updated_by = EXCLUDED.updated_by
        """

        item_dicts = [
            {
                "order_item_id": item.order_item_id,
                "order_id": item.order_id,
                "product_id": item.product_id,
                "product_name": item.product_name,
                "quantity": item.quanity,
                "price": item.unity_price,
                "order_status": item.order_status,
                "delivery_date": item.delivery_date,
                "created_at": item.created_at,
                "updated_at": item.updated_at,
                "created_by": item.created_by,
                "updated_by": item.updated_by
            }
            for item in order_items
        ]

        try:
            with conn.cursor() as cursor:
                execute_batch(
                    cursor, upsert_query, item_dicts, page_size=settings.BATCH_SIZE
                )
            conn.commit()
            logger.info(
                f"Loaded {len(order_items)} order items into the analytical database"
            )
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading order items: {e}")
            raise
