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

