# Tutorial End-to-End: Pipeline Analitika Data E-commerce dengan dbt

## Pendahuluan: Dari Data Mentah hingga Wawasan Prediktif

Tutorial ini mendemonstrasikan siklus hidup lengkap analitika data menggunakan dbt (Data Build Tool) dalam konteks e-commerce. Kita akan membangun pipeline yang mengikuti alur kerja profesional:

1. **Data Wrangling** - Pembersihan dan transformasi data mentah
2. **Data Aggregation** - Agregasi dan operasi grup untuk ringkasan bisnis
3. **Exploratory Data Analysis (EDA)** - Analisis visual untuk wawasan
4. **Predictive Modeling** - Persiapan data untuk model prediktif

**Studi Kasus**: Menganalisis perilaku pelanggan e-commerce untuk:
- Memprediksi customer lifetime value (CLV)
- Mengidentifikasi segmen pelanggan berisiko churn
- Mengoptimalkan strategi retensi pelanggan

---

## Arsitektur Pipeline

```
Raw Data (Seeds)           Staging Models              Intermediate Models         Marts
─────────────────          ───────────────             ───────────────────         ─────
raw_customers    ──────►   stg_customers    ──────►   int_customer_orders  ────►  fct_customer_metrics
raw_orders       ──────►   stg_orders       ──────►   int_customer_rfm     ────►  dim_customer_segments
raw_order_items  ──────►   stg_order_items  ─┘        int_product_metrics  ────►  fct_product_performance
raw_products     ──────►   stg_products     ───────►                              ml_customer_features
```

---

## Modul 1: Data Wrangling - Fondasi Pengolahan Data

### 1.1. Setup Data Mentah (Seeds)

Buat direktori dan file seed data:

```bash
mkdir -p seeds/raw
```

**seeds/raw/raw_customers.csv**
```csv
customer_id,email,first_name,last_name,country,created_at
1,john.doe@email.com,John,Doe,US,2023-01-15
2,jane.smith@email.com,Jane,Smith,UK,2023-02-20
3,bob.jones@email.com,Bob,Jones,US,2023-03-10
4,alice.wilson@email.com,Alice,Wilson,CA,2023-01-25
5,charlie.brown@email.com,Charlie,Brown,US,2023-04-05
```

**seeds/raw/raw_orders.csv**
```csv
order_id,customer_id,order_date,status,shipping_cost
1001,1,2023-02-01,completed,5.99
1002,1,2023-03-15,completed,5.99
1003,2,2023-03-01,completed,7.99
1004,3,2023-04-10,completed,5.99
1005,1,2023-05-20,cancelled,0.00
1006,2,2023-05-25,completed,7.99
1007,4,2023-06-01,completed,9.99
1008,3,2023-06-15,completed,5.99
1009,5,2023-07-01,completed,5.99
1010,1,2023-07-20,completed,5.99
```

**seeds/raw/raw_order_items.csv**
```csv
order_item_id,order_id,product_id,quantity,unit_price
1,1001,101,2,29.99
2,1001,102,1,49.99
3,1002,103,1,19.99
4,1003,101,3,29.99
5,1004,104,1,99.99
6,1006,102,2,49.99
7,1007,105,1,149.99
8,1008,103,2,19.99
9,1009,101,1,29.99
10,1010,104,1,99.99
```

**seeds/raw/raw_products.csv**
```csv
product_id,product_name,category,cost_price,list_price
101,Wireless Mouse,Electronics,15.00,29.99
102,Keyboard,Electronics,25.00,49.99
103,USB Cable,Accessories,5.00,19.99
104,Monitor,Electronics,50.00,99.99
105,Laptop Stand,Accessories,30.00,149.99
```

### 1.2. Staging Models - Pembersihan dan Standarisasi

**models/staging/stg_customers.sql**
```sql
{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ ref('raw_customers') }}
),

cleaned as (
    select
        customer_id,
        lower(trim(email)) as email,
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        upper(trim(country)) as country,
        cast(created_at as date) as customer_created_at,

        -- Data quality flags
        case
            when email is null or email = '' then false
            when email not like '%@%' then false
            else true
        end as is_valid_email,

        current_timestamp as dbt_loaded_at

    from source
)

select * from cleaned
where is_valid_email = true
```

**models/staging/stg_orders.sql**
```sql
{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ ref('raw_orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        lower(trim(status)) as order_status,
        cast(shipping_cost as decimal(10,2)) as shipping_cost,

        -- Derived fields
        extract(year from cast(order_date as date)) as order_year,
        extract(month from cast(order_date as date)) as order_month,
        extract(quarter from cast(order_date as date)) as order_quarter,
        dayname(cast(order_date as date)) as order_day_of_week,

        -- Data quality flags
        case when status = 'completed' then true else false end as is_completed,

        current_timestamp as dbt_loaded_at

    from source
)

select * from cleaned
```

**models/staging/stg_order_items.sql**
```sql
{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ ref('raw_order_items') }}
),

cleaned as (
    select
        order_item_id,
        order_id,
        product_id,
        cast(quantity as integer) as quantity,
        cast(unit_price as decimal(10,2)) as unit_price,
        cast(quantity * unit_price as decimal(10,2)) as line_total,

        current_timestamp as dbt_loaded_at

    from source
    where quantity > 0  -- Data quality filter
)

select * from cleaned
```

**models/staging/stg_products.sql**
```sql
{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ ref('raw_products') }}
),

cleaned as (
    select
        product_id,
        trim(product_name) as product_name,
        initcap(trim(category)) as category,
        cast(cost_price as decimal(10,2)) as cost_price,
        cast(list_price as decimal(10,2)) as list_price,
        cast(list_price - cost_price as decimal(10,2)) as profit_margin,
        cast((list_price - cost_price) / list_price * 100 as decimal(5,2)) as profit_margin_pct,

        current_timestamp as dbt_loaded_at

    from source
)

select * from cleaned
```

**models/staging/schema.yml**
```yaml
version: 2

models:
  - name: stg_customers
    description: Cleaned and standardized customer data
    columns:
      - name: customer_id
        description: Primary key for customers
        tests:
          - unique
          - not_null
      - name: email
        description: Customer email (cleaned and lowercased)
        tests:
          - not_null
      - name: is_valid_email
        description: Flag indicating email validity

  - name: stg_orders
    description: Cleaned and enriched order data
    columns:
      - name: order_id
        description: Primary key for orders
        tests:
          - unique
          - not_null
      - name: customer_id
        description: Foreign key to customers
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id

  - name: stg_order_items
    description: Cleaned order line items
    columns:
      - name: order_item_id
        tests:
          - unique
          - not_null
      - name: order_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_orders')
              field: order_id

  - name: stg_products
    description: Cleaned product data with profit calculations
    columns:
      - name: product_id
        tests:
          - unique
          - not_null
```

---

## Modul 2: Data Aggregation dan Operasi Grup

### 2.1. Intermediate Models - Agregasi Customer-Level

**models/intermediate/int_customer_orders.sql**
```sql
{{ config(
    materialized='table',
    tags=['intermediate', 'customer']
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
    where is_completed = true
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

order_totals as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.shipping_cost,
        sum(oi.line_total) as order_subtotal,
        sum(oi.line_total) + o.shipping_cost as order_total,
        count(oi.order_item_id) as items_count
    from orders o
    left join order_items oi on o.order_id = oi.order_id
    group by 1, 2, 3, 4
)

select * from order_totals
```

**models/intermediate/int_customer_rfm.sql**
```sql
{{ config(
    materialized='table',
    tags=['intermediate', 'customer', 'rfm']
) }}

-- RFM Analysis: Recency, Frequency, Monetary
with customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

analysis_date as (
    select max(order_date) as max_date
    from customer_orders
),

rfm_calc as (
    select
        co.customer_id,

        -- Recency: days since last purchase
        datediff('day', max(co.order_date), ad.max_date) as recency_days,

        -- Frequency: total number of orders
        count(distinct co.order_id) as frequency,

        -- Monetary: total revenue
        sum(co.order_total) as monetary_value,

        -- Additional metrics
        avg(co.order_total) as avg_order_value,
        min(co.order_date) as first_order_date,
        max(co.order_date) as last_order_date,
        datediff('day', min(co.order_date), max(co.order_date)) as customer_tenure_days

    from customer_orders co
    cross join analysis_date ad
    group by 1, ad.max_date
),

rfm_scores as (
    select
        *,

        -- RFM Scoring (1-5, where 5 is best)
        case
            when recency_days <= 30 then 5
            when recency_days <= 60 then 4
            when recency_days <= 90 then 3
            when recency_days <= 180 then 2
            else 1
        end as recency_score,

        case
            when frequency >= 5 then 5
            when frequency >= 4 then 4
            when frequency >= 3 then 3
            when frequency >= 2 then 2
            else 1
        end as frequency_score,

        case
            when monetary_value >= 500 then 5
            when monetary_value >= 300 then 4
            when monetary_value >= 200 then 3
            when monetary_value >= 100 then 2
            else 1
        end as monetary_score

    from rfm_calc
)

select
    *,
    (recency_score + frequency_score + monetary_score) as rfm_total_score,
    concat(recency_score, frequency_score, monetary_score) as rfm_segment_code
from rfm_scores
```

**models/intermediate/int_product_metrics.sql**
```sql
{{ config(
    materialized='table',
    tags=['intermediate', 'product']
) }}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
    where is_completed = true
),

products as (
    select * from {{ ref('stg_products') }}
),

product_sales as (
    select
        oi.product_id,
        count(distinct o.order_id) as orders_count,
        count(distinct o.customer_id) as customers_count,
        sum(oi.quantity) as units_sold,
        sum(oi.line_total) as revenue,
        avg(oi.unit_price) as avg_selling_price,
        min(o.order_date) as first_sale_date,
        max(o.order_date) as last_sale_date

    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    group by 1
)

select
    p.product_id,
    p.product_name,
    p.category,
    p.cost_price,
    p.list_price,
    p.profit_margin,
    p.profit_margin_pct,

    coalesce(ps.orders_count, 0) as orders_count,
    coalesce(ps.customers_count, 0) as customers_count,
    coalesce(ps.units_sold, 0) as units_sold,
    coalesce(ps.revenue, 0) as revenue,
    coalesce(ps.avg_selling_price, 0) as avg_selling_price,
    coalesce(ps.revenue - (ps.units_sold * p.cost_price), 0) as total_profit,
    ps.first_sale_date,
    ps.last_sale_date

from products p
left join product_sales ps on p.product_id = ps.product_id
```

---

## Modul 3: Data Marts untuk Analisis dan Visualisasi

### 3.1. Customer Metrics Mart

**models/marts/fct_customer_metrics.sql**
```sql
{{ config(
    materialized='table',
    tags=['marts', 'customer']
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

rfm as (
    select * from {{ ref('int_customer_rfm') }}
),

customer_profile as (
    select
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        c.country,
        c.customer_created_at,

        -- RFM Metrics
        r.recency_days,
        r.frequency,
        r.monetary_value,
        r.avg_order_value,
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.rfm_total_score,
        r.rfm_segment_code,

        -- Tenure
        r.customer_tenure_days,
        r.first_order_date,
        r.last_order_date,

        -- Customer Lifetime Value (simplified)
        r.monetary_value as current_clv,

        -- Predicted future value (simple heuristic)
        case
            when r.rfm_total_score >= 12 then r.monetary_value * 2.0
            when r.rfm_total_score >= 9 then r.monetary_value * 1.5
            when r.rfm_total_score >= 6 then r.monetary_value * 1.2
            else r.monetary_value * 0.8
        end as predicted_clv_12m,

        -- Churn risk
        case
            when r.recency_days > 180 and r.frequency = 1 then 'High'
            when r.recency_days > 120 then 'Medium'
            when r.recency_days > 90 then 'Low'
            else 'Very Low'
        end as churn_risk,

        current_timestamp as dbt_updated_at

    from customers c
    left join rfm r on c.customer_id = r.customer_id
)

select * from customer_profile
```

**models/marts/dim_customer_segments.sql**
```sql
{{ config(
    materialized='table',
    tags=['marts', 'customer', 'segmentation']
) }}

with customer_metrics as (
    select * from {{ ref('fct_customer_metrics') }}
),

segmentation as (
    select
        customer_id,
        email,
        first_name,
        last_name,

        -- Segment based on RFM scores
        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4 then 'Champions'
            when recency_score >= 3 and frequency_score >= 3 and monetary_score >= 3 then 'Loyal Customers'
            when recency_score >= 4 and frequency_score <= 2 and monetary_score <= 2 then 'New Customers'
            when recency_score >= 3 and frequency_score <= 2 and monetary_score >= 3 then 'Big Spenders'
            when recency_score <= 2 and frequency_score >= 3 and monetary_score >= 3 then 'At Risk'
            when recency_score <= 2 and frequency_score <= 2 and monetary_score >= 3 then 'Cant Lose Them'
            when recency_score <= 2 and frequency_score >= 2 and monetary_score <= 2 then 'Hibernating'
            when recency_score <= 1 and frequency_score <= 1 and monetary_score <= 1 then 'Lost'
            else 'Others'
        end as customer_segment,

        -- Segment characteristics
        recency_score,
        frequency_score,
        monetary_score,
        rfm_total_score,

        -- Business metrics
        monetary_value,
        frequency as total_orders,
        avg_order_value,
        predicted_clv_12m,
        churn_risk,

        -- Recommended actions
        case
            when recency_score >= 4 and frequency_score >= 4 then 'VIP treatment, exclusive offers'
            when recency_score >= 4 and frequency_score <= 2 then 'Onboarding campaign, product recommendations'
            when recency_score <= 2 and monetary_score >= 3 then 'Win-back campaign, special discount'
            when recency_score <= 2 and frequency_score <= 2 then 'Re-engagement or sunset'
            else 'Standard nurture campaign'
        end as recommended_action

    from customer_metrics
)

select * from segmentation
```

### 3.2. Product Performance Mart

**models/marts/fct_product_performance.sql**
```sql
{{ config(
    materialized='table',
    tags=['marts', 'product']
) }}

with product_metrics as (
    select * from {{ ref('int_product_metrics') }}
),

ranked_products as (
    select
        *,

        -- Rankings
        row_number() over (order by revenue desc) as revenue_rank,
        row_number() over (order by units_sold desc) as units_rank,
        row_number() over (order by total_profit desc) as profit_rank,

        -- Category share
        revenue / sum(revenue) over (partition by category) as category_revenue_share,

        -- Performance classification
        case
            when revenue >= (select percentile_cont(0.8) within group (order by revenue) from product_metrics) then 'Star'
            when revenue >= (select percentile_cont(0.5) within group (order by revenue) from product_metrics) then 'Performer'
            when revenue >= (select percentile_cont(0.2) within group (order by revenue) from product_metrics) then 'Average'
            else 'Underperformer'
        end as performance_tier

    from product_metrics
)

select * from ranked_products
```

---

## Modul 4: Machine Learning Feature Preparation

### 4.1. ML Features for Customer Prediction

**models/ml/ml_customer_features.sql**
```sql
{{ config(
    materialized='table',
    tags=['ml', 'features']
) }}

with customer_metrics as (
    select * from {{ ref('fct_customer_metrics') }}
),

customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

-- Behavioral features
customer_behavior as (
    select
        customer_id,

        -- Order patterns
        stddev(order_total) as order_value_volatility,
        max(order_total) as max_order_value,
        min(order_total) as min_order_value,

        -- Time-based patterns
        avg(datediff('day',
            lag(order_date) over (partition by customer_id order by order_date),
            order_date
        )) as avg_days_between_orders,

        -- Trend
        case
            when count(*) >= 3 then
                (sum(case when order_date >= dateadd('day', -90, current_date) then 1 else 0 end)::float /
                 sum(case when order_date >= dateadd('day', -180, current_date)
                          and order_date < dateadd('day', -90, current_date) then 1 else 0 end)::float)
            else null
        end as order_trend_ratio

    from customer_orders
    group by 1
),

-- Final feature set for ML
ml_features as (
    select
        cm.customer_id,

        -- Target variable (for supervised learning)
        case when cm.churn_risk in ('High', 'Medium') then 1 else 0 end as is_churn_risk,

        -- Demographic features
        cm.country,
        datediff('day', cm.customer_created_at, current_date) as account_age_days,

        -- RFM features
        cm.recency_days,
        cm.frequency,
        cm.monetary_value,
        cm.avg_order_value,
        cm.recency_score,
        cm.frequency_score,
        cm.monetary_score,
        cm.rfm_total_score,

        -- Derived features
        cm.customer_tenure_days,
        case when cm.frequency > 0 then cm.monetary_value / cm.frequency else 0 end as revenue_per_order,
        case when cm.customer_tenure_days > 0 then cm.frequency::float / cm.customer_tenure_days * 30 else 0 end as orders_per_month,

        -- Behavioral features
        cb.order_value_volatility,
        cb.max_order_value,
        cb.min_order_value,
        cb.avg_days_between_orders,
        cb.order_trend_ratio,

        -- Engagement features
        case when cm.recency_days <= 30 then 1 else 0 end as is_active_last_30d,
        case when cm.recency_days <= 90 then 1 else 0 end as is_active_last_90d,

        -- Value features
        cm.predicted_clv_12m,
        case when cm.monetary_value > 0 then cm.predicted_clv_12m / cm.monetary_value else 0 end as clv_growth_multiplier,

        current_timestamp as feature_generated_at

    from customer_metrics cm
    left join customer_behavior cb on cm.customer_id = cb.customer_id
)

select * from ml_features
```

**models/ml/schema.yml**
```yaml
version: 2

models:
  - name: ml_customer_features
    description: >
      Feature engineering for customer churn prediction and CLV modeling.
      This table contains normalized and engineered features ready for ML models.
    columns:
      - name: customer_id
        description: Unique customer identifier
        tests:
          - unique
          - not_null

      - name: is_churn_risk
        description: Target variable - 1 if customer is at medium/high churn risk, 0 otherwise

      - name: recency_days
        description: Days since last purchase (lower is better)

      - name: frequency
        description: Total number of orders

      - name: monetary_value
        description: Total revenue from customer

      - name: predicted_clv_12m
        description: Predicted customer lifetime value for next 12 months

      - name: order_trend_ratio
        description: Ratio of recent orders (last 90d) vs previous period (shows growth/decline)
```

---

## Modul 5: Analisis dan Visualisasi (Query Examples)

### 5.1. Customer Segmentation Analysis

```sql
-- Customer segment distribution
select
    customer_segment,
    count(*) as customer_count,
    sum(monetary_value) as total_revenue,
    avg(monetary_value) as avg_customer_value,
    avg(predicted_clv_12m) as avg_predicted_clv,
    sum(predicted_clv_12m) as total_predicted_value
from {{ ref('dim_customer_segments') }}
group by 1
order by total_revenue desc;
```

### 5.2. Churn Risk Analysis

```sql
-- Churn risk distribution by segment
select
    customer_segment,
    churn_risk,
    count(*) as customer_count,
    avg(monetary_value) as avg_value,
    avg(recency_days) as avg_recency
from {{ ref('dim_customer_segments') }}
group by 1, 2
order by 1, 2;
```

### 5.3. Product Performance Analysis

```sql
-- Top products by performance tier
select
    performance_tier,
    category,
    product_name,
    revenue,
    total_profit,
    profit_margin_pct,
    units_sold
from {{ ref('fct_product_performance') }}
where performance_tier in ('Star', 'Performer')
order by revenue desc
limit 20;
```

### 5.4. RFM Analysis Visualization Queries

```sql
-- RFM Score distribution (for heatmap)
select
    recency_score,
    frequency_score,
    count(*) as customer_count,
    avg(monetary_value) as avg_value
from {{ ref('fct_customer_metrics') }}
group by 1, 2
order by 1, 2;
```

---

## Modul 6: Testing dan Data Quality

### 6.1. Custom Tests

**tests/assert_positive_clv.sql**
```sql
-- Test that predicted CLV is always positive
select *
from {{ ref('fct_customer_metrics') }}
where predicted_clv_12m < 0
```

**tests/assert_rfm_scores_valid.sql**
```sql
-- Test that RFM scores are in valid range (1-5)
select *
from {{ ref('fct_customer_metrics') }}
where recency_score not between 1 and 5
   or frequency_score not between 1 and 5
   or monetary_score not between 1 and 5
```

### 6.2. Documentation

**models/marts/schema.yml**
```yaml
version: 2

models:
  - name: fct_customer_metrics
    description: >
      Comprehensive customer metrics including RFM analysis, CLV predictions,
      and churn risk assessment. This is the primary fact table for customer analytics.

    columns:
      - name: customer_id
        description: Primary key
        tests:
          - unique
          - not_null

      - name: monetary_value
        description: Total historical revenue from customer
        tests:
          - not_null

      - name: predicted_clv_12m
        description: Predicted customer lifetime value for next 12 months
        tests:
          - not_null

      - name: churn_risk
        description: Churn risk category (Very Low, Low, Medium, High)
        tests:
          - accepted_values:
              values: ['Very Low', 'Low', 'Medium', 'High']

  - name: dim_customer_segments
    description: >
      Customer segmentation based on RFM analysis with recommended actions
      for marketing and retention campaigns.

    columns:
      - name: customer_segment
        description: Business segment classification
        tests:
          - accepted_values:
              values: ['Champions', 'Loyal Customers', 'New Customers',
                      'Big Spenders', 'At Risk', 'Cant Lose Them',
                      'Hibernating', 'Lost', 'Others']
```

---

## Modul 7: Deployment dan Orchestration

### 7.1. Project Configuration

**dbt_project.yml**
```yaml
name: 'ecommerce_analytics'
version: '1.0.0'
config-version: 2

profile: 'default'

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
analysis-paths: ["analyses"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  ecommerce_analytics:
    staging:
      +materialized: view
      +tags: ['staging']

    intermediate:
      +materialized: table
      +tags: ['intermediate']

    marts:
      +materialized: table
      +tags: ['marts']

    ml:
      +materialized: table
      +tags: ['ml']

seeds:
  ecommerce_analytics:
    +schema: raw
```

### 7.2. Execution Commands

```bash
# Load seed data
dbt seed

# Run all models
dbt run

# Run tests
dbt test

# Run specific model with downstream dependencies
dbt run --select fct_customer_metrics+

# Run only ML feature preparation
dbt run --select tag:ml

# Generate documentation
dbt docs generate
dbt docs serve

# Full refresh
dbt run --full-refresh
```

### 7.3. Incremental Loading Strategy (Advanced)

**models/marts/fct_customer_metrics_incremental.sql**
```sql
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    tags=['marts', 'customer', 'incremental']
) }}

-- This version demonstrates incremental loading for production use
with customers as (
    select * from {{ ref('stg_customers') }}
    {% if is_incremental() %}
    where customer_created_at >= (select max(customer_created_at) from {{ this }})
    {% endif %}
),

-- ... rest of the logic same as fct_customer_metrics
```

---

## Modul 8: Business Use Cases

### 8.1. Use Case 1: Churn Prevention Campaign

**Objective**: Identify high-value customers at risk of churning

```sql
-- Target list for win-back campaign
select
    customer_id,
    email,
    first_name,
    last_name,
    customer_segment,
    monetary_value,
    recency_days,
    churn_risk,
    recommended_action
from {{ ref('dim_customer_segments') }}
where churn_risk in ('High', 'Medium')
  and monetary_value > 200  -- High value customers
  and customer_segment in ('At Risk', 'Cant Lose Them')
order by monetary_value desc;
```

### 8.2. Use Case 2: CLV-Based Marketing Budget Allocation

**Objective**: Allocate marketing budget based on predicted CLV

```sql
-- Marketing budget allocation by segment
select
    customer_segment,
    count(*) as customer_count,
    sum(predicted_clv_12m) as total_predicted_value,
    avg(predicted_clv_12m) as avg_predicted_clv,

    -- Suggested budget allocation (10% of predicted value)
    sum(predicted_clv_12m) * 0.10 as suggested_marketing_budget,

    -- Budget per customer
    sum(predicted_clv_12m) * 0.10 / count(*) as budget_per_customer

from {{ ref('dim_customer_segments') }}
group by 1
order by total_predicted_value desc;
```

### 8.3. Use Case 3: Product Recommendation Engine (Data Prep)

```sql
-- Customer-Product affinity matrix for collaborative filtering
with customer_product_purchases as (
    select
        o.customer_id,
        oi.product_id,
        count(*) as purchase_count,
        sum(oi.line_total) as total_spent
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_order_items') }} oi on o.order_id = oi.order_id
    where o.is_completed = true
    group by 1, 2
)

select * from customer_product_purchases;
```

---

## Kesimpulan dan Best Practices

### Alur Kerja yang Telah Dibangun

1. **Data Wrangling** ✓
   - Staging layer untuk cleaning dan standardisasi
   - Data quality checks dengan tests
   - Type casting dan null handling

2. **Data Aggregation** ✓
   - RFM analysis untuk customer segmentation
   - Product performance metrics
   - Customer behavior patterns

3. **Analytical Marts** ✓
   - Customer metrics dengan CLV dan churn risk
   - Customer segmentation untuk targeting
   - Product performance untuk inventory

4. **ML Feature Engineering** ✓
   - Normalized features untuk modeling
   - Target variable preparation
   - Time-based feature engineering

### Best Practices yang Diterapkan

1. **Modular Design**: Separation of concerns (staging → intermediate → marts)
2. **Data Quality**: Comprehensive testing at each layer
3. **Documentation**: Schema.yml untuk setiap model
4. **Incremental Logic**: Strategi untuk production scaling
5. **Business Logic**: Clear mapping dari data ke keputusan bisnis

### Next Steps untuk Production

1. **Orchestration**: Setup dengan Airflow/Dagster
2. **Monitoring**: Data quality dashboard
3. **CI/CD**: Automated testing dan deployment
4. **ML Integration**: Export features ke Python untuk training
5. **Visualization**: BI tool integration (Metabase, Tableau, Looker)

---

## Appendix: Quick Reference

### dbt Commands Cheat Sheet

```bash
# Development
dbt compile              # Compile without running
dbt run --select model   # Run specific model
dbt test --select model  # Test specific model

# Model selection
dbt run --select +model  # Model and all upstream
dbt run --select model+  # Model and all downstream
dbt run --select tag:staging

# Production
dbt run --target prod
dbt test --target prod
dbt run --full-refresh

# Documentation
dbt docs generate
dbt docs serve --port 8001
```

### SQL Patterns Reference

```sql
-- Window functions
row_number() over (partition by x order by y)
lag(col) over (partition by x order by y)

-- Aggregations with conditions
sum(case when condition then value else 0 end)

-- Percentiles
percentile_cont(0.8) within group (order by col)

-- Date functions
datediff('day', start_date, end_date)
dateadd('day', -90, current_date)
```
