from typing import Dict, List, Tuple, Any
from models.schema import OrderData, OrderItemData
from utils.logger import logger


class DataTransformer:
    """Class to transform data for the analytical database."""

    def transform_data(
        self,
        orders: List[Dict[str, Any]],
        order_items: List[Dict[str, Any]],
        customers: List[Dict[str, Any]],
        products: List[Dict[str, Any]],
    ) -> Tuple[List[OrderData], List[OrderItemData]]:
        """Transform data into analytical models."""
        # Create lookup dictionaries
        customer_dict = {c["customer_id"]: c for c in customers}
        product_dict = {p["product_id"]: p for p in products}

        # Group order items by order_id
        order_items_by_order = {}
        for item in order_items:
            order_id = item["order_id"]
            if order_id not in order_items_by_order:
                order_items_by_order[order_id] = []
            order_items_by_order[order_id].append(item)

        # Transform orders
        transformed_orders = []
        transformed_order_items = []

        for order in orders:
            order_id = order["order_id"]
            customer_id = order["customer_id"]
            customer = customer_dict.get(customer_id, {"customer_name": "Unknown"})

            # Get items for this order
            items = order_items_by_order.get(order_id, [])

            # Calculate totals
            total_items = sum(item["quanity"] for item in items)
            total_amount = sum(item["quanity"] * product_dict[item["product_id"]]["unity_price"] for item in items)
            logger.debug(f"Processing order: {order}")
            # Create order data
            try:
                
                order_data = OrderData(
                    order_id=int(order["order_id"]),
                    customer_id=int(order["customer_id"]),
                    customer_name=customer["customer_name"],
                    order_date=order["order_date"],
                    delivery_date=order["delivery_date"],
                    status=order["status"],
                    total_items=int(total_items),
                    total_amount=float(total_amount),
                    created_at=order["created_at"],
                    updated_at=order["updated_at"],
                    created_by=str(order["created_by"]),
                    updated_by=str(order["updated_by"]),
                    items=[],
                )
                logger.debug(f"Created order data: {order_data.__dict__}")
            except Exception as e:
                logger.error(f"Error creating order data: {e}")
                logger.error(f"Order data: {order}")
                logger.error(f"Customer data: {customer}")
                raise

            # Transform order items
            for item in items:
                product_id = int(item["product_id"])
                product = product_dict.get(product_id, {
                    "product_name": "Unknown",
                    "unity_price": 0.0
                })

                item_data = OrderItemData(
                    order_item_id=int(item["order_item_id"]),
                    order_id=int(order_id),
                    product_id=product_id,
                    product_name=product["product_name"],
                    quanity=int(item["quanity"]),
                    unity_price=float(product["unity_price"]),
                    order_status=order["status"],
                    delivery_date=order["delivery_date"],
                    created_at=item["created_at"],
                    updated_at=item["updated_at"],
                    created_by=str(item["created_by"]),
                    updated_by=str(item["updated_by"]),
                )

                order_data.items.append(item_data)
                transformed_order_items.append(item_data)

            transformed_orders.append(order_data)

        logger.info(
            f"Transformed {len(transformed_orders)} orders and {len(transformed_order_items)} order items"
        )
        return transformed_orders, transformed_order_items
