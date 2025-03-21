import time
import click
from utils.logger import logger
from config.settings import settings


def run_continuous_etl():
    """Run the ETL process continuously."""
    from etl.extract import DataExtractor
    from etl.transform import DataTransformer
    from etl.load import DataLoader

    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DataLoader()

    try:
        # check if the schema exists
        loader.ensure_schema_exists()

        logger.info("Starting continuous ETL process")

        while True:
            try:
                # Extract data
                orders, order_items, customers, products = (
                    extractor.extract_data_batch()
                )

                if orders:
                    # Transform data
                    transformed_orders, transformed_order_items = (
                        transformer.transform_data(
                            orders, order_items, customers, products
                        )
                    )

                    # Load data
                    loader.load_orders(transformed_orders)
                    loader.load_order_items(transformed_order_items)

                    logger.info(f"Processed {len(transformed_orders)} orders")
                else:
                    logger.info("No new data to process")

                # Sleep for polling interval
                time.sleep(settings.POLLING_INTERVAL)
            except Exception as e:
                logger.error(f"Error in ETL iteration: {e}")
                # Sleep before retry
                time.sleep(settings.POLLING_INTERVAL)
    except KeyboardInterrupt:
        logger.info("ETL process interrupted by user")
    finally:
        extractor.close()
        loader.close()


if __name__ == "__main__":
    from cli.commands import cli

    cli()
