from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timezone
from utils.database import get_source_connection, execute_query
from utils.logger import logger


class DataExtractor:
    """Class to extract data from the source database."""

    def __init__(self):
        self.conn = None
        self.last_extraction_time = datetime.min.replace(tzinfo=timezone.utc)

    def connect(self):
        """Connect to the source database."""
        if self.conn is None or self.conn.closed:
            self.conn = get_source_connection()
        return self.conn

    def close(self):
        """Close the connection to the source database."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Connection to source database closed")

    def extract_orders(self, since: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Extract orders from the source database."""
        conn = self.connect()

        query = """
        SELECT o.order_id, o.customer_id, o.order_date, o.delivery_date, o.status, 
               o.created_at, o.updated_at, o.created_by, o.updated_by
        FROM operations.orders o
        WHERE o.updated_at > %s
        ORDER BY o.updated_at
        """

        since = since or self.last_extraction_time
        params = (since,)

        try:
            results = execute_query(conn, query, params)
            if results:
                self.last_extraction_time = max(r["updated_at"] for r in results)
            logger.info(f"Extracted {len(results)} orders since {since}")
            return results
        except Exception as e:
            logger.error(f"Error extracting orders: {e}")
            raise

    def extract_order_items(self, order_ids: List[int]) -> List[Dict[str, Any]]:
        """Extract order items for the given order IDs."""
        if not order_ids:
            return []

        conn = self.connect()

        placeholders = ",".join(["%s"] * len(order_ids))
        query = f"""
        SELECT oi.order_item_id, oi.order_id, oi.product_id, oi.quanity,
               oi.created_at, oi.updated_at, oi.created_by, oi.updated_by
        FROM operations.order_items oi
        WHERE oi.order_id IN ({placeholders})
        """

        try:
            results = execute_query(conn, query, tuple(order_ids))
            logger.info(
                f"Extracted {len(results)} order items for {len(order_ids)} orders"
            )
            return results
        except Exception as e:
            logger.error(f"Error extracting order items: {e}")
            raise

    def extract_customers(self, customer_ids: List[int]) -> List[Dict[str, Any]]:
        """Extract customers for the given customer IDs."""
        if not customer_ids:
            return []

        conn = self.connect()

        placeholders = ",".join(["%s"] * len(customer_ids))
        query = f"""
        SELECT c.customer_id, c.customer_name, c.is_active, c.customer_address,
               c.created_at, c.updated_at, c.created_by, c.updated_by
        FROM operations.customers c
        WHERE c.customer_id IN ({placeholders})
        """

        try:
            results = execute_query(conn, query, tuple(customer_ids))
            logger.info(f"Extracted {len(results)} customers")
            return results
        except Exception as e:
            logger.error(f"Error extracting customers: {e}")
            raise

    def extract_products(self, product_ids: List[int]) -> List[Dict[str, Any]]:
        """Extract products for the given product IDs."""
        if not product_ids:
            return []

        conn = self.connect()

        placeholders = ",".join(["%s"] * len(product_ids))
        query = f"""
        SELECT p.product_id, p.product_name, p.barcode, p.unity_price, p.is_active,
               p.created_at, p.updated_at, p.created_by, p.updated_by
        FROM operations.products p
        WHERE p.product_id IN ({placeholders})
        """

        try:
            results = execute_query(conn, query, tuple(product_ids))
            logger.info(f"Extracted {len(results)} products")
            return results
        except Exception as e:
            logger.error(f"Error extracting products: {e}")
            raise

    def extract_data_batch(
        self,
    ) -> Tuple[
        List[Dict[str, Any]],
        List[Dict[str, Any]],
        List[Dict[str, Any]],
        List[Dict[str, Any]],
    ]:
        """Extract a batch of data from the source database."""
        # Extract orders
        orders = self.extract_orders()

        if not orders:
            return [], [], [], []

        # Extract related data
        order_ids = [o["order_id"] for o in orders]
        customer_ids = list(set(o["customer_id"] for o in orders))

        order_items = self.extract_order_items(order_ids)
        customers = self.extract_customers(customer_ids)

        product_ids = list(set(oi["product_id"] for oi in order_items))
        products = self.extract_products(product_ids)

        return orders, order_items, customers, products
