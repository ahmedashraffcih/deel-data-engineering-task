import os
import csv
from typing import Dict, List, Any, Optional
import click
from datetime import datetime
from utils.database import get_target_connection, execute_query
from utils.logger import logger
from config.settings import settings



class AnalyticsQueries:
    """Class to run analytical queries on the target database."""

    def __init__(self):
        self.conn = None

    def connect(self):
        """Connect to the target database."""
        if self.conn is None or self.conn.closed:
            self.conn = get_target_connection()
        return self.conn

    def close(self):
        """Close the connection to the target database."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Connection to target database closed")

    def get_open_orders_by_date_status(self) -> List[Dict[str, Any]]:
        """Get number of open orders by delivery date and status."""
        conn = self.connect()

        query = """
        SELECT
            delivery_date::date,
            status,
            COUNT(*) as order_count
        FROM analytics.analytical_orders
        WHERE status <> 'COMPLETED'
        AND delivery_date IS NOT NULL
        GROUP BY
        delivery_date::date, status
        ORDER BY
        delivery_date::date, status
        """

        try:
            results = execute_query(conn, query)
            logger.info(
                f"Retrieved {len(results)} records for open orders by date and status"
            )
            return results
        except Exception as e:
            logger.error(f"Error running open_orders_by_date_status query: {e}")
            raise

    def get_top_delivery_dates_with_open_orders(
        self, limit: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Get top delivery dates with more open orders.
        """
        conn = self.connect()

        query = f"""
        SELECT
            delivery_date::date,
            COUNT(*) as order_count,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM 
            analytics.analytical_orders
        WHERE
            status <> 'COMPLETED'
            AND delivery_date IS NOT NULL
        GROUP BY
            delivery_date::date
        ORDER BY
            order_count DESC
        LIMIT {limit}
        """

        try:
            results = execute_query(conn, query)
            logger.info(f"Retrieved top {limit} delivery dates with more open orders")
            return results
        except Exception as e:
            logger.error(f"Error running top_delivery_dates query: {e}")
            raise

    def get_pending_items_by_product(self) -> List[Dict[str, Any]]:
        """Get number of open pending items by product ID."""
        conn = self.connect()

        query = """
        SELECT
            oi.product_id,
            oi.product_name,
            SUM(oi.quantity) as pending_items
        FROM
            analytics.analytical_order_items oi
        WHERE
            oi.order_status = 'PENDING'
        GROUP BY
            oi.product_id, oi.product_name
        ORDER BY
            pending_items DESC
        """

        try:
            results = execute_query(conn, query)
            logger.info(
                f"Retrieved {len(results)} records for pending items by product"
            )
            return results
        except Exception as e:
            logger.error(f"Error running pending_items_by_product query: {e}")
            raise

    def get_top_customers_with_pending_orders(
        self, limit: int = 3
    ) -> List[Dict[str, Any]]:
        """Get top customers with more pending orders."""
        conn = self.connect()

        query = f"""
        SELECT
            o.customer_id,
            o.customer_name,
            COUNT(*) as pending_order_count,
            SUM(o.total_amount) as total_pending_amount
        FROM
            analytics.analytical_orders o
        WHERE
            o.status = 'PENDING'
        GROUP BY
            o.customer_id, o.customer_name
        ORDER BY
            pending_order_count DESC
        LIMIT {limit}
        """

        try:
            results = execute_query(conn, query)
            logger.info(f"Retrieved top {limit} customers with more pending orders")
            return results
        except Exception as e:
            logger.error(f"Error running top_customers_with_pending_orders query: {e}")
            raise

    # def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
    #     """Execute a custom query and return results."""
    #     conn = self.connect()
    #     try:
    #         with conn.cursor() as cursor:
    #             if params:
    #                 cursor.execute(query, params)
    #             else:
    #                 cursor.execute(query)
    #             return cursor.fetchall()
    #     except Exception as e:
    #         logger.error(f"Error executing query: {e}")
    #         raise


def export_to_csv(data: List[Dict[str, Any]], filename: str):
    """Export data to a CSV file."""
    if not data:
        logger.warning(f"No data to export to {filename}")
        return

    # Ensure output directory exists
    os.makedirs(settings.OUTPUT_DIRECTORY, exist_ok=True)

    # Full path for the file
    filepath = os.path.join(settings.OUTPUT_DIRECTORY, filename)

    # Get column headers from the first record
    fieldnames = list(data[0].keys())

    try:
        with open(filepath, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        logger.info(f"Data exported to {filepath}")
    except Exception as e:
        logger.error(f"Error exporting to CSV {filepath}: {e}")
        raise

