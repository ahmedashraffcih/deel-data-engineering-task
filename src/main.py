import time
import click
from utils.logger import logger
from config.settings import settings

def run_etl():
    """Run the ETL process to sync data from source to analytics database."""
    from etl.extract import DataExtractor
    from etl.transform import DataTransformer
    from etl.load import DataLoader

    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DataLoader()

    try:
        # Ensure analytics schema exists
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


if __name__ == "__main__":
    run_etl()
