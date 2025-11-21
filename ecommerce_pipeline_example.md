# Realistic Data Pipeline: E-commerce Analytics with dbt

## Business Context

You work for **ShopSmart**, a growing e-commerce company that sells electronics, clothing, and home goods. The business stakeholders need insights into:

- **Revenue Analytics**: Daily/weekly/monthly revenue trends
- **Customer Analytics**: Customer lifetime value, retention, segmentation
- **Product Analytics**: Best-selling products, inventory turnover, pricing optimization
- **Marketing Analytics**: Campaign effectiveness, conversion rates

## Raw Data Sources

For this tutorial, we'll use CSV files that simulate real e-commerce data. This makes it easy to follow along without setting up a database!

### CSV Data Files (seeds/)
- `raw_customers.csv` - Customer registration data
- `raw_orders.csv` - Order transactions
- `raw_order_items.csv` - Individual items within orders
- `raw_products.csv` - Product catalog
- `raw_categories.csv` - Product categories

**Benefits of using CSV files:**
- ✅ **Instant setup**: No database required
- ✅ **Version controlled**: Data changes tracked in Git
- ✅ **Portable**: Share datasets easily with team
- ✅ **Perfect for learning**: Focus on dbt concepts, not infrastructure

## Project Setup

Let's build this pipeline step by step:

### 1. Prerequisites
```bash
# Install dbt with DuckDB
pip install dbt-core dbt-duckdb

# Verify installation
dbt --version
```

### 2. Initialize the dbt Project

```bash
dbt init my_first_project
cd my_first_project
```

### 3. Configure DuckDB Connection

`profiles.yml`:
```yaml
my_first_project:
  outputs:
    dev:
      type: duckdb
      path: 'my_dbt.duckdb'
      threads: 4
    prod:
      type: duckdb
      path: 'my_dbt_prod.duckdb'
      threads: 8
  target: dev
```

**Why DuckDB?**
- 🚀 **Fast**: Columnar storage, vectorized execution
- 💾 **Lightweight**: Single file database
- 🔧 **Zero maintenance**: No server to manage
- 📊 **Analytics optimized**: Built for analytical queries

### 4. Project Configuration

`dbt_project.yml`:
```yaml
name: 'my_first_project'
version: '1.0.0'
config-version: 2

profile: 'my_first_project'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  my_first_project:
    staging:
      +materialized: view
      +tags: ["staging"]
    intermediate:
      +materialized: ephemeral
      +tags: ["intermediate"]
    marts:
      +materialized: table
      +tags: ["marts"]
      core:
        +tags: ["core", "daily"]
      marketing:
        +tags: ["marketing", "weekly"]

vars:
  # Date variables for incremental models
  start_date: '2023-01-01'
  # Business logic variables
  high_value_customer_threshold: 1000
  active_customer_days: 90
```

## Sample Data Creation

Let's create realistic sample data for our e-commerce pipeline:

### 1. Create CSV Files

`seeds/raw_categories.csv`:
```csv
category_id,category_name,parent_category
1,Electronics,
2,Smartphones,Electronics
3,Laptops,Electronics
4,Clothing,
5,Men's Clothing,Clothing
6,Women's Clothing,Clothing
7,Home & Garden,
8,Kitchen,Home & Garden
```

`seeds/raw_products.csv`:
```csv
product_id,product_name,category_id,brand,price,cost,sku,is_active,created_at
101,iPhone 14 Pro,2,Apple,999.00,700.00,APPL-IP14P-128,true,2023-01-15
102,Samsung Galaxy S23,2,Samsung,849.00,600.00,SAMS-GS23-256,true,2023-01-20
103,MacBook Air M2,3,Apple,1299.00,900.00,APPL-MBA-M2,true,2023-02-01
104,Dell XPS 13,3,Dell,1099.00,750.00,DELL-XPS13-512,true,2023-02-15
105,Men's Jeans,5,Levi's,89.99,35.00,LEVI-JEANS-32,true,2023-01-10
106,Women's Dress,6,Zara,79.99,25.00,ZARA-DRESS-M,true,2023-01-25
107,Coffee Maker,8,Keurig,149.99,75.00,KEUR-CM-K55,true,2023-03-01
108,Blender,8,Vitamix,399.99,200.00,VITA-BLEND-5200,true,2023-03-05
```

`seeds/raw_customers.csv`:
```csv
user_id,email,first_name,last_name,date_of_birth,phone,city,state,country,status,created_at,updated_at
1,john.doe@email.com,John,Doe,1985-06-15,555-0101,New York,NY,USA,active,2023-01-15,2023-01-15
2,jane.smith@email.com,Jane,Smith,1990-03-22,555-0102,Los Angeles,CA,USA,active,2023-01-20,2023-01-20
3,bob.johnson@email.com,Bob,Johnson,1978-11-08,555-0103,Chicago,IL,USA,active,2023-02-01,2023-02-01
4,alice.brown@email.com,Alice,Brown,1995-07-12,555-0104,Houston,TX,USA,active,2023-02-15,2023-02-15
5,charlie.wilson@email.com,Charlie,Wilson,1988-09-30,555-0105,Phoenix,AZ,USA,inactive,2023-03-01,2023-03-01
6,diana.davis@email.com,Diana,Davis,1992-12-03,555-0106,Philadelphia,PA,USA,active,2023-03-10,2023-03-10
7,eve.miller@email.com,Eve,Miller,1983-04-18,555-0107,San Antonio,TX,USA,active,2023-03-15,2023-03-15
8,frank.garcia@email.com,Frank,Garcia,1991-01-25,555-0108,San Diego,CA,USA,active,2023-03-20,2023-03-20
```

`seeds/raw_orders.csv`:
```csv
order_id,user_id,order_date,order_status,payment_method,shipping_method,order_total,shipping_cost,tax_amount,discount_amount,coupon_code,created_at,updated_at
1001,1,2023-01-20,delivered,credit_card,standard,999.00,9.99,79.92,0.00,,2023-01-20,2023-01-25
1002,2,2023-01-25,delivered,paypal,express,849.00,19.99,67.92,50.00,WELCOME10,2023-01-25,2023-01-28
1003,1,2023-02-05,delivered,credit_card,standard,1299.00,0.00,103.92,0.00,,2023-02-05,2023-02-10
1004,3,2023-02-10,delivered,credit_card,standard,169.98,5.99,13.60,0.00,,2023-02-10,2023-02-15
1005,4,2023-02-20,shipped,paypal,express,79.99,9.99,6.40,8.00,SAVE10,2023-02-20,2023-02-22
1006,2,2023-03-05,delivered,credit_card,standard,549.98,7.99,44.00,55.00,LOYALTY15,2023-03-05,2023-03-10
1007,6,2023-03-15,delivered,credit_card,express,1099.00,19.99,87.92,0.00,,2023-03-15,2023-03-18
1008,7,2023-03-25,pending,paypal,standard,229.98,5.99,18.40,0.00,,2023-03-25,2023-03-25
1009,1,2023-04-01,delivered,credit_card,standard,399.99,0.00,32.00,0.00,,2023-04-01,2023-04-05
1010,8,2023-04-10,cancelled,credit_card,standard,89.99,9.99,7.20,0.00,,2023-04-10,2023-04-10
```

`seeds/raw_order_items.csv`:
```csv
order_item_id,order_id,product_id,quantity,unit_price,created_at,updated_at
1,1001,101,1,999.00,2023-01-20,2023-01-20
2,1002,102,1,849.00,2023-01-25,2023-01-25
3,1003,103,1,1299.00,2023-02-05,2023-02-05
4,1004,105,1,89.99,2023-02-10,2023-02-10
5,1004,106,1,79.99,2023-02-10,2023-02-10
6,1005,106,1,79.99,2023-02-20,2023-02-20
7,1006,107,1,149.99,2023-03-05,2023-03-05
8,1006,108,1,399.99,2023-03-05,2023-03-05
9,1007,104,1,1099.00,2023-03-15,2023-03-15
10,1008,105,1,89.99,2023-03-25,2023-03-25
11,1008,107,1,149.99,2023-03-25,2023-03-25
12,1009,108,1,399.99,2023-04-01,2023-04-01
13,1010,105,1,89.99,2023-04-10,2023-04-10
```

## Data Pipeline Architecture

### Layer 1: Seeds (Raw Data)

`models/sources.yml`:
```yaml
version: 2

seeds:
  - name: raw_customers
    description: Customer registration and profile data from CSV
    columns:
      - name: user_id
        description: Primary key
        tests:
          - unique
          - not_null
      - name: email
        description: User email address
        tests:
          - unique
          - not_null
      - name: created_at
        description: Registration date
        tests:
          - not_null
      - name: status
        description: User account status
        tests:
          - accepted_values:
              values: ['active', 'inactive', 'suspended']

  - name: raw_orders
    description: Order transaction data from CSV
    columns:
      - name: order_id
        description: Primary key
        tests:
          - unique
          - not_null
      - name: user_id
        description: Foreign key to users
        tests:
          - not_null
          - relationships:
              to: ref('raw_customers')
              field: user_id
      - name: order_status
        description: Current order status
        tests:
          - accepted_values:
              values: ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned']
      - name: order_total
        description: Total order amount
        tests:
          - not_null

  - name: raw_order_items
    description: Individual items within each order from CSV
    columns:
      - name: order_item_id
        description: Primary key
        tests:
          - unique
          - not_null
      - name: order_id
        description: Foreign key to orders
        tests:
          - relationships:
              to: ref('raw_orders')
              field: order_id
      - name: product_id
        description: Foreign key to products
        tests:
          - relationships:
              to: ref('raw_products')
              field: product_id

  - name: raw_products
    description: Product catalog from CSV
    columns:
      - name: product_id
        description: Primary key
        tests:
          - unique
          - not_null
      - name: product_name
        description: Product name
        tests:
          - not_null
      - name: category_id
        description: Foreign key to categories
        tests:
          - relationships:
              to: ref('raw_categories')
              field: category_id
      - name: price
        description: Current selling price
        tests:
          - not_null
      - name: cost
        description: Product cost
        tests:
          - not_null

  - name: raw_categories
    description: Product categories from CSV
    columns:
      - name: category_id
        description: Primary key
        tests:
          - unique
          - not_null
      - name: category_name
        description: Category name
        tests:
          - not_null
```

### Layer 2: Staging Models (Data Cleaning)

`models/staging/stg_ecommerce__users.sql`:
```sql
WITH source_data AS (
    SELECT * FROM {{ ref('raw_customers') }}
),

cleaned AS (
    SELECT
        user_id,
        LOWER(TRIM(email)) as email,
        TRIM(first_name) as first_name,
        TRIM(last_name) as last_name,
        CASE 
            WHEN first_name IS NOT NULL AND last_name IS NOT NULL 
            THEN TRIM(first_name || ' ' || last_name)
            ELSE COALESCE(first_name, last_name, 'Unknown')
        END as full_name,
        date_of_birth,
        CASE 
            WHEN date_of_birth IS NOT NULL 
            THEN DATE_PART('year', AGE(date_of_birth))
            ELSE NULL
        END as age,
        phone,
        address,
        city,
        state,
        country,
        zipcode,
        status,
        created_at::DATE as registration_date,
        updated_at,
        -- Data quality flags
        CASE 
            WHEN email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' 
            THEN TRUE 
            ELSE FALSE 
        END as is_valid_email,
        CASE 
            WHEN phone ~ '^[\+]?[0-9\-\(\)\s]+$' 
            THEN TRUE 
            ELSE FALSE 
        END as is_valid_phone
    FROM source_data
    WHERE created_at IS NOT NULL
)

SELECT * FROM cleaned
```

`models/staging/stg_ecommerce__orders.sql`:
```sql
WITH source_data AS (
    SELECT * FROM {{ ref('raw_orders') }}
),

cleaned AS (
    SELECT
        order_id,
        user_id,
        order_date::DATE as order_date,
        order_status,
        -- Standardize status values
        CASE 
            WHEN order_status IN ('confirmed', 'shipped', 'delivered') THEN 'completed'
            WHEN order_status IN ('cancelled', 'returned') THEN 'cancelled'
            ELSE 'pending'
        END as order_status_grouped,
        payment_method,
        shipping_method,
        CAST(order_total AS DECIMAL(10,2)) as order_total,
        CAST(shipping_cost AS DECIMAL(10,2)) as shipping_cost,
        CAST(tax_amount AS DECIMAL(10,2)) as tax_amount,
        CAST(discount_amount AS DECIMAL(10,2)) as discount_amount,
        -- Calculate net revenue (after discounts, before tax)
        CAST(order_total - COALESCE(discount_amount, 0) AS DECIMAL(10,2)) as net_order_total,
        coupon_code,
        created_at,
        updated_at,
        -- Business logic fields
        EXTRACT(YEAR FROM order_date) as order_year,
        EXTRACT(MONTH FROM order_date) as order_month,
        EXTRACT(DOW FROM order_date) as order_day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM order_date) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END as order_day_type
    FROM source_data
    WHERE order_date IS NOT NULL
      AND order_total > 0
)

SELECT * FROM cleaned
```

`models/staging/stg_ecommerce__order_items.sql`:
```sql
WITH source_data AS (
    SELECT * FROM {{ ref('raw_order_items') }}
),

cleaned AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        CAST(quantity AS INTEGER) as quantity,
        CAST(unit_price AS DECIMAL(10,2)) as unit_price,
        CAST(quantity * unit_price AS DECIMAL(10,2)) as line_total,
        created_at,
        updated_at
    FROM source_data
    WHERE quantity > 0 
      AND unit_price > 0
)

SELECT * FROM cleaned
```

`models/staging/stg_ecommerce__products.sql`:
```sql
WITH source_data AS (
    SELECT * FROM {{ ref('raw_products') }}
),

cleaned AS (
    SELECT
        product_id,
        TRIM(product_name) as product_name,
        -- product_description, -- Not in our CSV
        category_id,
        brand,
        CAST(price AS DECIMAL(10,2)) as price,
        CAST(cost AS DECIMAL(10,2)) as cost,
        CAST(price - cost AS DECIMAL(10,2)) as profit_margin,
        -- weight, dimensions, color, size, -- Not in our CSV
        sku,
        is_active,
        created_at,
        updated_at
    FROM source_data
    WHERE product_name IS NOT NULL
)

SELECT * FROM cleaned
```

### Layer 3: Intermediate Models (Business Logic)

`models/intermediate/int_customer_order_history.sql`:
```sql
WITH orders AS (
    SELECT * FROM {{ ref('stg_ecommerce__orders') }}
),

users AS (
    SELECT * FROM {{ ref('stg_ecommerce__users') }}
),

customer_orders AS (
    SELECT
        u.user_id,
        u.email,
        u.full_name,
        u.registration_date,
        o.order_id,
        o.order_date,
        o.order_status,
        o.order_status_grouped,
        o.net_order_total,
        -- Calculate days between registration and first order
        CASE 
            WHEN o.order_date = MIN(o.order_date) OVER (PARTITION BY u.user_id)
            THEN o.order_date - u.registration_date
            ELSE NULL
        END as days_to_first_order,
        -- Order sequence number for each customer
        ROW_NUMBER() OVER (
            PARTITION BY u.user_id 
            ORDER BY o.order_date, o.order_id
        ) as order_sequence_number
    FROM users u
    LEFT JOIN orders o ON u.user_id = o.user_id
    WHERE o.order_status_grouped IN ('completed', 'delivered')
)

SELECT * FROM customer_orders
```

`models/intermediate/int_product_performance.sql`:
```sql
WITH order_items AS (
    SELECT * FROM {{ ref('stg_ecommerce__order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_ecommerce__orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_ecommerce__products') }}
),

categories AS (
    SELECT * FROM {{ ref('raw_categories') }}
),

product_sales AS (
    SELECT
        oi.product_id,
        p.product_name,
        p.brand,
        c.category_name,
        p.price as current_price,
        p.cost as current_cost,
        COUNT(DISTINCT oi.order_id) as total_orders,
        SUM(oi.quantity) as total_quantity_sold,
        SUM(oi.line_total) as total_revenue,
        AVG(oi.unit_price) as avg_selling_price,
        SUM(oi.line_total) / NULLIF(SUM(oi.quantity), 0) as revenue_per_unit,
        MIN(o.order_date) as first_sale_date,
        MAX(o.order_date) as last_sale_date
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    WHERE o.order_status_grouped IN ('completed', 'delivered')
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT * FROM product_sales
```

### Layer 4: Data Marts (Final Analytics Tables)

`models/marts/core/dim_customers.sql`:
```sql
WITH customer_history AS (
    SELECT * FROM {{ ref('int_customer_order_history') }}
),

customer_metrics AS (
    SELECT
        user_id,
        email,
        full_name,
        registration_date,
        COUNT(order_id) as total_orders,
        SUM(net_order_total) as lifetime_value,
        AVG(net_order_total) as avg_order_value,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        MAX(order_date) - MIN(order_date) as customer_lifespan_days,
        -- Recency metrics
        CURRENT_DATE - MAX(order_date) as days_since_last_order,
        -- Customer segmentation
        CASE 
            WHEN SUM(net_order_total) >= {{ var('high_value_customer_threshold') }} THEN 'High Value'
            WHEN COUNT(order_id) >= 5 THEN 'Frequent'
            WHEN COUNT(order_id) = 1 THEN 'One-time'
            ELSE 'Regular'
        END as customer_segment,
        CASE 
            WHEN CURRENT_DATE - MAX(order_date) <= {{ var('active_customer_days') }} THEN 'Active'
            WHEN CURRENT_DATE - MAX(order_date) <= 180 THEN 'At Risk'
            ELSE 'Inactive'
        END as customer_status
    FROM customer_history
    WHERE order_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

final AS (
    SELECT
        user_id,
        email,
        full_name,
        registration_date,
        first_order_date,
        last_order_date,
        total_orders,
        lifetime_value,
        avg_order_value,
        customer_lifespan_days,
        days_since_last_order,
        customer_segment,
        customer_status,
        -- Additional calculated fields
        CASE 
            WHEN total_orders > 1 THEN lifetime_value / (total_orders - 1)
            ELSE NULL
        END as avg_repeat_order_value,
        CASE 
            WHEN customer_lifespan_days > 0 THEN total_orders::DECIMAL / (customer_lifespan_days / 30.0)
            ELSE NULL
        END as orders_per_month
    FROM customer_metrics
)

SELECT * FROM final
```

`models/marts/core/fct_orders.sql`:
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_ecommerce__orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_ecommerce__order_items') }}
),

customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

order_summary AS (
    SELECT
        o.order_id,
        o.user_id,
        o.order_date,
        o.order_status,
        o.order_status_grouped,
        o.payment_method,
        o.shipping_method,
        o.order_total,
        o.shipping_cost,
        o.tax_amount,
        o.discount_amount,
        o.net_order_total,
        o.coupon_code,
        o.order_year,
        o.order_month,
        o.order_day_of_week,
        o.order_day_type,
        -- Order item aggregations
        COUNT(oi.order_item_id) as total_items,
        SUM(oi.quantity) as total_quantity,
        AVG(oi.unit_price) as avg_item_price,
        -- Customer context
        c.customer_segment,
        c.customer_status,
        c.total_orders as customer_total_orders,
        c.lifetime_value as customer_lifetime_value,
        -- Order sequence for this customer
        ROW_NUMBER() OVER (
            PARTITION BY o.user_id 
            ORDER BY o.order_date, o.order_id
        ) as customer_order_sequence
    FROM orders o
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN customers c ON o.user_id = c.user_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
)

SELECT * FROM order_summary

{% if is_incremental() %}
    WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

`models/marts/core/fct_order_items.sql`:
```sql
WITH order_items AS (
    SELECT * FROM {{ ref('stg_ecommerce__order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_ecommerce__orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_ecommerce__products') }}
),

categories AS (
    SELECT * FROM {{ ref('raw_categories') }}
),

final AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        o.user_id,
        o.order_date,
        o.order_status_grouped,
        oi.product_id,
        p.product_name,
        p.brand,
        c.category_name,
        oi.quantity,
        oi.unit_price,
        p.current_price,
        p.current_cost,
        oi.line_total,
        -- Profitability calculations
        oi.quantity * p.current_cost as line_cost,
        oi.line_total - (oi.quantity * p.current_cost) as line_profit,
        CASE 
            WHEN p.current_cost > 0 
            THEN (oi.line_total - (oi.quantity * p.current_cost)) / oi.line_total 
            ELSE NULL 
        END as profit_margin_pct,
        -- Pricing analysis
        CASE 
            WHEN oi.unit_price > p.current_price THEN 'Above Current Price'
            WHEN oi.unit_price < p.current_price THEN 'Below Current Price'
            ELSE 'At Current Price'
        END as price_comparison
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    WHERE o.order_status_grouped IN ('completed', 'delivered')
)

SELECT * FROM final
```

### Layer 5: Analytics Marts

`models/marts/marketing/marketing_performance.sql`:
```sql
WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
),

daily_metrics AS (
    SELECT
        order_date,
        order_day_of_week,
        order_day_type,
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT user_id) as unique_customers,
        SUM(net_order_total) as total_revenue,
        AVG(net_order_total) as avg_order_value,
        SUM(total_items) as total_items_sold,
        SUM(total_quantity) as total_quantity_sold,
        -- New vs returning customers
        COUNT(DISTINCT CASE WHEN customer_order_sequence = 1 THEN user_id END) as new_customers,
        COUNT(DISTINCT CASE WHEN customer_order_sequence > 1 THEN user_id END) as returning_customers,
        -- Payment methods
        COUNT(CASE WHEN payment_method = 'credit_card' THEN 1 END) as credit_card_orders,
        COUNT(CASE WHEN payment_method = 'paypal' THEN 1 END) as paypal_orders,
        COUNT(CASE WHEN payment_method = 'bank_transfer' THEN 1 END) as bank_transfer_orders
    FROM orders
    WHERE order_status_grouped IN ('completed', 'delivered')
    GROUP BY 1, 2, 3
),

final AS (
    SELECT
        *,
        -- Calculate running totals and moving averages
        SUM(total_revenue) OVER (
            ORDER BY order_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as running_total_revenue,
        AVG(total_revenue) OVER (
            ORDER BY order_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as revenue_7day_avg,
        AVG(total_orders) OVER (
            ORDER BY order_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as orders_7day_avg,
        -- Calculate growth rates
        LAG(total_revenue) OVER (ORDER BY order_date) as prev_day_revenue,
        CASE 
            WHEN LAG(total_revenue) OVER (ORDER BY order_date) > 0
            THEN (total_revenue - LAG(total_revenue) OVER (ORDER BY order_date)) 
                 / LAG(total_revenue) OVER (ORDER BY order_date) * 100
            ELSE NULL
        END as revenue_growth_pct
    FROM daily_metrics
)

SELECT * FROM final
```

## Advanced Features

### 1. Macros for Reusable Logic

`macros/get_customer_segment.sql`:
```sql
{% macro get_customer_segment(lifetime_value_column, order_count_column) %}
    CASE 
        WHEN {{ lifetime_value_column }} >= {{ var('high_value_customer_threshold') }} THEN 'High Value'
        WHEN {{ order_count_column }} >= 5 THEN 'Frequent'
        WHEN {{ order_count_column }} = 1 THEN 'One-time'
        ELSE 'Regular'
    END
{% endmacro %}
```

### 2. Custom Tests

`tests/assert_order_total_equals_sum_of_items.sql`:
```sql
WITH order_totals AS (
    SELECT
        o.order_id,
        o.order_total,
        SUM(oi.line_total) as calculated_total,
        ABS(o.order_total - SUM(oi.line_total)) as difference
    FROM {{ ref('stg_ecommerce__orders') }} o
    JOIN {{ ref('stg_ecommerce__order_items') }} oi 
        ON o.order_id = oi.order_id
    GROUP BY 1, 2
)

SELECT *
FROM order_totals
WHERE difference > 0.01  -- Allow for rounding differences
```

### 3. Snapshots for Historical Tracking

`snapshots/customers_snapshot.sql`:
```sql
{% snapshot customers_snapshot %}
    {{
        config(
          target_schema='snapshots',
          unique_key='user_id',
          strategy='timestamp',
          updated_at='updated_at'
        )
    }}

    SELECT * FROM {{ source('ecommerce_db', 'users') }}
    
{% endsnapshot %}
```

### 4. Seeds for Static Data

`seeds/product_categories_mapping.csv`:
```csv
category_id,category_name,category_group,is_seasonal
1,Electronics,Technology,FALSE
2,Clothing,Fashion,TRUE
3,Home & Garden,Home,FALSE
4,Sports,Lifestyle,TRUE
```

## Testing Strategy

`models/marts/schema.yml`:
```yaml
version: 2

models:
  - name: dim_customers
    description: "Customer dimension with lifetime metrics and segmentation"
    tests:
      - unique:
          column_name: user_id
      - not_null:
          column_name: user_id
    columns:
      - name: lifetime_value
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100000
      - name: customer_segment
        tests:
          - accepted_values:
              values: ['High Value', 'Frequent', 'One-time', 'Regular']

  - name: fct_orders
    tests:
      - unique:
          column_name: order_id
      - dbt_utils.expression_is_true:
          expression: "net_order_total >= 0"
    columns:
      - name: order_date
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: "'2020-01-01'"
              max_value: "current_date"
```

## Deployment and Orchestration

### 1. Production Deployment
```bash
# Run in production
dbt run --target prod

# Run specific marts
dbt run --models marts.core --target prod

# Run with full refresh
dbt run --full-refresh --models fct_orders --target prod
```

### 2. CI/CD with dbt Cloud
```yaml
# .github/workflows/dbt.yml
name: dbt CI
on:
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup dbt
        uses: dbt-labs/setup-dbt@v1
        with:
          dbt-version: 1.5.0
      - name: Install dependencies
        run: dbt deps
      - name: Run tests
        run: dbt test
```

## Quick Start: Running the Complete Pipeline

Follow these steps to run the entire pipeline:

```bash
# 1. Navigate to your project
cd my_first_project

# 2. Load all CSV data into DuckDB
dbt seed

# 3. Run all models (staging → intermediate → marts)
dbt run

# 4. Test data quality
dbt test

# 5. Generate documentation
dbt docs generate
dbt docs serve  # Opens in browser at http://localhost:8080

# 6. View sample results
dbt show --select dim_customers --limit 5
dbt show --select marketing_performance --limit 10
```

### Key Files Structure
```
my_first_project/
├── dbt_project.yml
├── profiles.yml
├── seeds/
│   ├── raw_customers.csv
│   ├── raw_orders.csv
│   ├── raw_order_items.csv
│   ├── raw_products.csv
│   └── raw_categories.csv
├── models/
│   ├── sources.yml
│   ├── staging/
│   │   ├── stg_ecommerce__users.sql
│   │   ├── stg_ecommerce__orders.sql
│   │   ├── stg_ecommerce__order_items.sql
│   │   └── stg_ecommerce__products.sql
│   ├── intermediate/
│   │   ├── int_customer_order_history.sql
│   │   └── int_product_performance.sql
│   └── marts/
│       ├── core/
│       │   ├── dim_customers.sql
│       │   ├── fct_orders.sql
│       │   └── fct_order_items.sql
│       └── marketing/
│           └── marketing_performance.sql
└── target/  # Generated files (git ignored)
```

### Sample Queries You Can Run

After running the pipeline, try these queries in your DuckDB database:

```sql
-- Top 5 customers by lifetime value
SELECT full_name, lifetime_value, total_orders, customer_segment
FROM dim_customers 
ORDER BY lifetime_value DESC 
LIMIT 5;

-- Daily revenue trends
SELECT order_date, total_revenue, total_orders, avg_order_value
FROM marketing_performance 
ORDER BY order_date DESC 
LIMIT 10;

-- Product profitability analysis
SELECT product_name, brand, total_revenue, line_profit, profit_margin_pct
FROM fct_order_items 
WHERE line_profit IS NOT NULL
ORDER BY line_profit DESC 
LIMIT 10;
```

## Business Value and Key Metrics

This pipeline enables analysis of:

### Revenue Analytics
- Daily, weekly, monthly revenue trends
- Revenue by customer segment, product category
- Impact of discounts and promotions on profitability

### Customer Analytics
- Customer lifetime value distribution
- Customer retention and churn analysis
- Customer acquisition cost and payback period

### Product Analytics
- Best-selling products and categories
- Profit margin analysis by product
- Inventory turnover rates

### Marketing Analytics
- Campaign effectiveness and ROI
- Channel attribution analysis
- Customer acquisition funnel metrics

## Next Steps for Production

1. **Data Quality Monitoring**: Implement alerting for test failures
2. **Performance Optimization**: Add indexes, partitioning for large tables
3. **Advanced Analytics**: Add cohort analysis, predictive models
4. **Real-time Streaming**: Integrate with Kafka/streaming for real-time metrics
5. **Data Governance**: Implement column-level lineage and PII handling

This comprehensive example demonstrates how dbt enables modern data teams to build scalable, tested, and documented analytics pipelines that directly support business decision-making.
