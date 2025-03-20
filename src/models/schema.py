from typing import List, Optional
from dataclasses import dataclass
from datetime import datetime, timezone
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = {"schema": "operations"}

    order_id = sa.Column(sa.Integer, primary_key=True)
    customer_id = sa.Column(sa.Integer, nullable=False)
    order_date = sa.Column(sa.Date)
    delivery_date = sa.Column(sa.Date)
    status = sa.Column(sa.String)
    created_at = sa.Column(
        sa.DateTime(3), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at = sa.Column(
        sa.DateTime(3), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    created_by = sa.Column(sa.String)
    updated_by = sa.Column(sa.String)


class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = {"schema": "operations"}

    order_item_id = sa.Column(sa.Integer, primary_key=True)
    order_id = sa.Column(sa.Integer, nullable=False)
    product_id = sa.Column(sa.Integer, nullable=False)
    quanity = sa.Column(sa.Integer)  # Note: typo in DDL is maintained
    created_at = sa.Column(
        sa.DateTime(3), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at = sa.Column(
        sa.DateTime(3), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    created_by = sa.Column(sa.String)
    updated_by = sa.Column(sa.String)


class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = {"schema": "operations"}

    customer_id = sa.Column(sa.Integer, primary_key=True)
    customer_name = sa.Column(sa.String(500), nullable=False)
    is_active = sa.Column(sa.Boolean, nullable=False, default=True)
    customer_address = sa.Column(sa.String(500))
    created_at = sa.Column(
        sa.DateTime(3), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at = sa.Column(
        sa.DateTime(3), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    created_by = sa.Column(sa.String)
    updated_by = sa.Column(sa.String)


class Product(Base):
    __tablename__ = "products"
    __table_args__ = {"schema": "operations"}

    product_id = sa.Column(sa.Integer, primary_key=True)
    product_name = sa.Column(sa.String(500), nullable=False)
    barcode = sa.Column(sa.String(26), nullable=False)
    unity_price = sa.Column(sa.Numeric, nullable=False)
    is_active = sa.Column(sa.Boolean)
    created_at = sa.Column(
        sa.DateTime(3), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at = sa.Column(
        sa.DateTime(3), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    created_by = sa.Column(sa.String)
    updated_by = sa.Column(sa.String)

# Analytical database models
class AnalyticalOrder(Base):
    __tablename__ = "analytical_orders"
    __table_args__ = {"schema": "analytics"}

    order_id = sa.Column(sa.Integer, primary_key=True)
    customer_id = sa.Column(sa.Integer, nullable=False)
    customer_name = sa.Column(sa.String(100), nullable=False)
    order_date = sa.Column(sa.DateTime, nullable=False)
    delivery_date = sa.Column(sa.DateTime)
    status = sa.Column(sa.String(20), nullable=False)
    total_items = sa.Column(sa.Integer, nullable=False)
    total_amount = sa.Column(sa.Numeric(10, 2), nullable=False)
    created_at = sa.Column(sa.DateTime, nullable=False)
    updated_at = sa.Column(sa.DateTime, nullable=False)
    created_by = sa.Column(sa.String, nullable=False)
    updated_by = sa.Column(sa.String, nullable=False)


class AnalyticalOrderItem(Base):
    __tablename__ = "analytical_order_items"
    __table_args__ = {"schema": "analytics"}

    id = sa.Column(sa.Integer, primary_key=True)
    order_id = sa.Column(sa.Integer, nullable=False)
    product_id = sa.Column(sa.Integer, nullable=False)
    product_name = sa.Column(sa.String(100), nullable=False)
    quantity = sa.Column(sa.Integer, nullable=False)
    price = sa.Column(sa.Numeric(10, 2), nullable=False)
    order_status = sa.Column(sa.String(20), nullable=False)
    delivery_date = sa.Column(sa.DateTime)
    created_at = sa.Column(sa.DateTime, nullable=False)
    updated_at = sa.Column(sa.DateTime, nullable=False)
    created_by = sa.Column(sa.String, nullable=False)
    updated_by = sa.Column(sa.String, nullable=False)


# Data classes for transformation layer
@dataclass
class OrderData:
    order_id: int
    customer_id: int
    customer_name: str
    order_date: datetime
    delivery_date: Optional[datetime]
    status: str
    total_items: int
    total_amount: float
    created_at: datetime
    updated_at: datetime
    created_by: str
    updated_by: str
    items: List["OrderItemData"]


@dataclass
class OrderItemData:
    order_item_id: int
    order_id: int
    product_id: int
    product_name: str
    quanity: int
    unity_price: float
    order_status: str
    delivery_date: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    created_by: str
    updated_by: str
