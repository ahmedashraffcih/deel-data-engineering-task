# ACME Delivery Services Analytics Platform

This project implements a data engineering solution for ACME Delivery Services to monitor created orders and their details with low latency between the orders system and the analytical platform.

## Overview

The solution extracts data from a source PostgreSQL database, transforms it into an analytical model, and loads it into a target PostgreSQL database. 
A command-line interface (CLI) allows users to run queries and export results to CSV files.

### Key Features

- Modular design with clear separation of concerns
- Near real-time data synchronization
- Efficient data extraction and transformation
- Command-line interface for business queries
- Docker-based deployment for easy setup
- Comprehensive error handling and logging

## Architecture

The solution follows a modular architecture with these components:

1. **Extraction Layer**: Connects to the source database and extracts data incrementally
2. **Transformation Layer**: Transforms the raw data into an analytical model
3. **Loading Layer**: Loads the transformed data into the analytical database
4. **CLI Application**: Provides a user interface to query and export results

## Project Structure

```
acme-analytics/
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── src/
│   ├── main.py
└── ├── README.md
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py
│   │   └── logging.dev.ini
│   ├── etl/
│   │   ├── __init__.py
│   │   └── extract.py
│   │   └── transform.py
│   │   └── load.py
│   ├── models/
│   │   ├── __init__.py
│   │   └── schema.py
│   ├── cli/
│   │   ├── __init__.py
│   │   └── commands.py
│   └── utils/
│       ├── __init__.py
│       ├── database.py
│       └── logger.py
├── tests/
│   ├── __init__.py
│   ├── test_extraction.py
│   ├── test_transformation.py
│   └── test_loading.py
├── requirements.txt
├── .env
├── Dockerfile
├── docker-compose.yaml
├── entrypoint.sh
└── README.md
```
## Installation and Setup

### Prerequisites

- Docker and Docker Compose
- Git

### Setup Instructions

1. Start the Docker containers:

```bash
docker-compose up -d
```

This will start the following services:
- PostgreSQL database
- Application service (app)
## Usage

### Running the ETL Process

You have several options to run the ETL process:

1. **One-time ETL run**:
```bash
python -m src.cli run-etl
```

2. **Continuous ETL** (with default polling interval):
```bash
python -m src.cli run-continuous-etl
```

3. **Continuous ETL** (with custom polling interval):
```bash
python -m src.cli run-continuous-etl --polling-interval 30
```


### Generating Reports via CLI

To export all reports:
```bash
python -m src.cli export-reports --all
```

Or export specific reports:
```bash
python -m src.cli export-reports --open-orders
python -m src.cli export-reports --top-dates
python -m src.cli export-reports --pending-items
python -m src.cli export-reports --top-customers
```

The reports will be saved in the `output` directory as CSV files.

### Available Reports

1. **Open Orders by Date and Status**: Number of open orders by DELIVERY_DATE and STATUS
2. **Top Delivery Dates**: Top 3 delivery dates with more open orders
3. **Pending Items by Product**: Number of open pending items by PRODUCT_ID
4. **Top Customers**: Top 3 Customers with more pending orders

## Design Considerations

### Data Model

The analytical database uses a denormalized schema optimized for the required business queries:

- `analytical_orders`: Contains order information with customer details
- `analytical_order_items`: Contains order item information with product details

### Incremental Processing

The solution extracts data incrementally based on update timestamps to minimize load on the source database.

### Error Handling

Comprehensive error handling and logging are implemented throughout the application to ensure reliability.

### Configuration

Configuration is managed through environment variables and the `.env` file, allowing easy customization.

## ETL Pipeline
## 1. Data Extraction (`src/etl/extract.py`)
The extraction phase pulls data from the operational database using the `DataExtractor` class.

### Key Components:
1. **Order Extraction**
```python
def extract_orders(self, since: Optional[datetime] = None):
```
- Extracts orders updated since the last extraction time
- Retrieves: order_id, customer_id, dates, status, and audit fields
- Uses incremental loading to avoid processing unchanged data
- Tracks last_extraction_time to ensure data consistency

2. **Order Items Extraction**
```python
def extract_order_items(self, order_ids: List[int]):
```
- Pulls order items for extracted orders
- Retrieves: order_item_id, order_id, product_id, quantity
- Uses batch processing with IN clause for efficiency

3. **Customer Data Extraction**
```python
def extract_customers(self, customer_ids: List[int]):
```
- Fetches customer details for relevant orders
- Retrieves: customer_id, name, status, address
- Optimized with batch retrieval

## 2. Data Transformation (`src/etl/transform.py`)
The transformation phase converts operational data into analytical models using the `DataTransformer` class.

### Key Steps:
1. **Data Preparation**
```python
# Create lookup dictionaries
customer_dict = {c["customer_id"]: c for c in customers}
product_dict = {p["product_id"]: p for p in products}

# Group order items by order
order_items_by_order = {}
for item in order_items:
    order_id = item["order_id"]
    if order_id not in order_items_by_order:
        order_items_by_order[order_id] = []
    order_items_by_order[order_id].append(item)
```

2. **Order Transformation**
- Calculates order totals:
  ```python
  total_items = sum(item["quanity"] for item in items)
  total_amount = sum(item["quanity"] * product_dict[item["product_id"]]["unity_price"])
  ```
- Creates analytical order records with:
  - Order details (ID, dates, status)
  - Customer information
  - Calculated totals
  - Audit information

3. **Order Items Transformation**
- Transforms each order item with:
  - Product details
  - Order status
  - Delivery information
  - Audit trails

## 3. Data Loading (`src/etl/load.py`)
The loading phase uses the `DataLoader` class to populate the analytical database.

### Schema Setup:
```sql
CREATE TABLE analytics.analytical_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    delivery_date TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    total_items INTEGER NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    -- Audit fields
);

CREATE TABLE analytics.analytical_order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    -- Additional fields
);
```
### Optimization Features:
1. **Indexes for Performance**
```sql
CREATE INDEX idx_orders_status ON analytics.analytical_orders (status);
CREATE INDEX idx_orders_delivery_date ON analytics.analytical_orders (delivery_date);
CREATE INDEX idx_items_product_id ON analytics.analytical_order_items (product_id);
CREATE INDEX idx_items_order_status ON analytics.analytical_order_items (order_status);
```

2. **Upsert Operations**
- Uses ON CONFLICT DO UPDATE for idempotent loads
- Maintains data consistency during reruns

## 4. Analytics Queries (`src/cli/commands.py`)
The `AnalyticsQueries` class implements four key analytical views:

### 1. Open Orders by Date/Status
```sql
SELECT 
    delivery_date::date, 
    status, 
    COUNT(*) as order_count,
    SUM(total_amount) as total_amount
FROM 
    analytics.analytical_orders
WHERE 
    status NOT IN ('COMPLETED', 'CANCELLED')
GROUP BY 
    delivery_date::date, status
```
- Tracks open orders distribution
- Monitors order value by status

### 2. Top Delivery Dates
```sql
SELECT 
    delivery_date::date,
    COUNT(*) as order_count,
    COUNT(DISTINCT customer_id) as unique_customers
FROM 
    analytics.analytical_orders
WHERE 
    status NOT IN ('COMPLETED', 'CANCELLED')
GROUP BY 
    delivery_date::date
ORDER BY 
    order_count DESC
LIMIT 3
```
- Identifies high-volume delivery dates
- Helps in capacity planning

### 3. Pending Items by Product
```sql
SELECT 
    oi.product_id,
    oi.product_name,
    SUM(oi.quantity) as pending_quantity,
    SUM(oi.quantity * oi.price) as total_pending_value
FROM 
    analytics.analytical_order_items oi
WHERE 
    oi.order_status = 'PENDING'
GROUP BY 
    oi.product_id, oi.product_name
```
- Tracks pending inventory needs
- Monitors product-level backlog

### 4. Top Customers with Pending Orders
```sql
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
LIMIT 3
```
- Identifies high-priority customers
- Tracks pending order value




## Future Improvements

1. **CDC Implementation**: Enhance the Change Data Capture implementation for real-time updates
2. **Data Versioning**:
   - Track changes to orders over time
   - Maintain historical status changes
   - Implement SCD (Slowly Changing Dimensions) for customer data
3. **Data Quality and Validation**:
   - Add data validation rules and constraints
   - Implement data quality scoring
   - Add data profiling and anomaly detection
   - Create data quality dashboards
   - Add unit tests for transformation logic
4. **Performance Optimization**:
   - Implement table partitioning by date for better query performance
   - Add materialized views for frequently accessed analytics
   - Implement query optimization and caching strategies
   - Add connection pooling for better database performance
5. **Partitioning**: Implement table partitioning for improved query performance on large datasets
6. **API Layer**: Add a REST API to expose the analytical data
7. **Visualization**: Integrate with a visualization tool like Grafana, Tableau, or through streamlit app

## Troubleshooting

### Common Issues

1. **Database Connection Errors**: Ensure the database services are running and the credentials are correct
2. **Missing Data**: Check the ETL logs for errors during the extraction or loading process
3. **Permission Issues**: Ensure the Docker volumes have the correct permissions

### Logs

Logs are organized by component and stored in the `logs` directory. You can access logs in several ways:
1. **Application Logs**:
```bash
# View all application logs
tail -f logs/*.log

# View specific date's logs
tail -f logs/YYYYMMDD_*.log
```

2. **Docker Container Logs**:
```bash
# View all container logs
docker-compose logs

# View specific service logs
docker-compose logs app
docker-compose logs transactions-db

# Follow log output in real-time
docker-compose logs -f app
```