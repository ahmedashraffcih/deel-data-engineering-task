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


@click.group()
def cli():
    pass


@cli.command()
def setup_analytics_db():
    """Initialize the analytics database schema."""
    from etl.load import DataLoader

    loader = DataLoader()
    try:
        loader.ensure_schema_exists()
        click.echo("Analytics database schema setup completed.")
    finally:
        loader.close()


@cli.command()
def run_etl():
    """Run the ETL process to sync data from source to analytics database."""
    from etl.extract import DataExtractor
    from etl.transform import DataTransformer
    from etl.load import DataLoader

    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DataLoader()

    try:
        loader.ensure_schema_exists()

        # Extract data
        orders, order_items, customers, products = extractor.extract_data_batch()

        if not orders:
            click.echo("No new data to process.")
            return

        # Transform data
        transformed_orders, transformed_order_items = transformer.transform_data(
            orders, order_items, customers, products
        )

        # Load data
        loader.load_orders(transformed_orders)
        loader.load_order_items(transformed_order_items)

        click.echo(
            f"ETL process completed. Processed {len(transformed_orders)} orders."
        )
    except Exception as e:
        logger.error(f"Error in ETL process: {e}")
        click.echo(f"Error in ETL process: {e}")
    finally:
        extractor.close()
        loader.close()


@cli.command()
@click.option("--all", is_flag=True, help="Export all reports")
@click.option(
    "--open-orders", is_flag=True, help="Export open orders by date and status"
)
@click.option(
    "--top-dates", is_flag=True, help="Export top delivery dates with open orders"
)
@click.option("--pending-items", is_flag=True, help="Export pending items by product")
@click.option(
    "--top-customers", is_flag=True, help="Export top customers with pending orders"
)
def export_reports(all, open_orders, top_dates, pending_items, top_customers):
    """Export analytical reports to CSV files."""
    queries = AnalyticsQueries()

    try:
        if all or open_orders:
            data = queries.get_open_orders_by_date_status()
            export_to_csv(data, "open_orders_by_date_status.csv")
            click.echo("Exported open orders by date and status.")

        if all or top_dates:
            data = queries.get_top_delivery_dates_with_open_orders()
            export_to_csv(data, "top_delivery_dates.csv")
            click.echo("Exported top delivery dates with open orders.")

        if all or pending_items:
            data = queries.get_pending_items_by_product()
            export_to_csv(data, "pending_items_by_product.csv")
            click.echo("Exported pending items by product.")

        if all or top_customers:
            data = queries.get_top_customers_with_pending_orders()
            export_to_csv(data, "top_customers_with_pending_orders.csv")
            click.echo("Exported top customers with pending orders.")

        if not any([all, open_orders, top_dates, pending_items, top_customers]):
            click.echo(
                "No reports selected for export. Use --all or specify individual reports."
            )
    except Exception as e:
        logger.error(f"Error exporting reports: {e}")
        click.echo(f"Error exporting reports: {e}")
    finally:
        queries.close()


@cli.command()
@click.option('--polling-interval', default=None, type=int, help='Override the polling interval in seconds')
def run_continuous_etl(polling_interval):
    """Run the ETL process continuously."""
    if polling_interval:
        settings.POLLING_INTERVAL = polling_interval
    
    from main import run_continuous_etl as continuous_etl
    continuous_etl()
