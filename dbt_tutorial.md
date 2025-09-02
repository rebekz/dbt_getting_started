# dbt (data build tool) Tutorial for Data Engineering

## Table of Contents
1. [What is dbt?](#what-is-dbt)
2. [Why Use dbt?](#why-use-dbt)
3. [Installation & Setup](#installation--setup)
4. [Core Concepts](#core-concepts)
5. [Project Structure](#project-structure)
6. [Your First dbt Model](#your-first-dbt-model)
7. [Materializations](#materializations)
8. [Jinja Templating](#jinja-templating)
9. [Testing](#testing)
10. [Documentation](#documentation)
11. [Best Practices](#best-practices)

## What is dbt?

dbt (data build tool) is a transformation workflow that helps data teams transform data in their warehouse more effectively. It allows you to:

- Write SQL SELECT statements and dbt handles the DDL/DML
- Version control your analytics code
- Test your data transformations
- Document your data models
- Build modular, reusable data models

## Why Use dbt?

### Traditional Problems in Data Transformation
- SQL scripts scattered across different systems
- No version control for analytics logic
- Difficult to test data quality
- Poor documentation and lineage tracking
- Hard to collaborate on data transformations

### dbt Solutions
- **Version Control**: Git-based workflow for analytics code
- **Testing**: Built-in data quality testing framework
- **Documentation**: Auto-generated docs with lineage graphs
- **Modularity**: Reusable models and macros
- **Collaboration**: Code reviews and team development

## Installation & Setup

### Option 1: dbt Core with DuckDB (Recommended for Learning)

DuckDB is perfect for learning dbt as it requires no setup and works with local CSV files!

1. **Install dbt Core with DuckDB**
```bash
# Install dbt with DuckDB adapter - lightweight and local
pip install dbt-core dbt-duckdb
```

2. **Verify Installation**
```bash
dbt --version
```

### Why DuckDB for Learning?
- ✅ **Zero setup**: No database server required
- ✅ **Local files**: Works directly with CSV files
- ✅ **Fast**: In-memory processing for quick iteration
- ✅ **SQL compatible**: Full SQL support like traditional warehouses

### Option 2: dbt Cloud (Recommended for Beginners)
- Sign up at [cloud.getdbt.com](https://cloud.getdbt.com)
- Browser-based IDE with built-in version control
- Managed scheduling and monitoring

## Core Concepts

### 1. Models
- SQL files that define transformations
- Each model is a SELECT statement
- dbt turns models into tables or views

### 2. Sources
- Raw data tables in your warehouse
- Defined in YAML files
- Enable testing and documentation of source data

### 3. Seeds
- CSV files with static data
- Version-controlled lookup tables
- Loaded into your warehouse with `dbt seed`

### 4. Tests
- Assertions about your data
- Schema tests (built-in): unique, not_null, accepted_values, relationships
- Data tests (custom SQL)

### 5. Snapshots
- Capture historical changes in mutable tables
- Implement slowly changing dimensions (SCD Type 2)

## Project Structure

```
my_dbt_project/
├── dbt_project.yml          # Project configuration
├── models/                  # SQL model files
│   ├── staging/            # Raw data cleaning
│   ├── intermediate/       # Business logic
│   ├── marts/             # Final business tables
│   └── schema.yml         # Tests and documentation
├── seeds/                 # CSV files
├── snapshots/             # Historical data capture
├── tests/                 # Custom data tests
├── macros/                # Reusable SQL functions
├── analysis/              # Ad-hoc queries
└── target/                # Compiled SQL (generated)
```

## Your First dbt Model

### 1. Initialize a dbt Project
```bash
dbt init my_first_project
cd my_first_project
```

### 2. Configure DuckDB Connection
Edit `profiles.yml` in your home directory:

```yaml
my_first_project:
  outputs:
    dev:
      type: duckdb
      path: 'my_dbt.duckdb'  # Local database file
      threads: 4
  target: dev
```

**That's it!** DuckDB will create a local database file automatically.

### 3. Create Sample Data
First, let's create a CSV file with sample data:

`seeds/raw_users.csv`:
```csv
id,first_name,last_name,email,created_at
1,John,Doe,john.doe@email.com,2023-01-15
2,Jane,Smith,jane.smith@email.com,2023-02-20
3,Bob,Johnson,bob.johnson@email.com,2023-03-10
4,Alice,Brown,alice.brown@email.com,2023-04-05
5,Charlie,Wilson,charlie.wilson@email.com,2023-05-12
```

### 4. Create Your First Model
Create `models/my_first_model.sql`:

```sql
-- models/my_first_model.sql
SELECT 
    id,
    first_name,
    last_name,
    email,
    first_name || ' ' || last_name as full_name,
    UPPER(first_name || ' ' || last_name) as full_name_upper,
    created_at,
    CURRENT_TIMESTAMP as transformed_at,
    -- Calculate days since registration
    CURRENT_DATE - CAST(created_at AS DATE) as days_since_registration
FROM {{ ref('raw_users') }}
WHERE created_at IS NOT NULL
```

### 5. Define Seeds and Models
Create `models/schema.yml`:

```yaml
version: 2

seeds:
  - name: raw_users
    description: Raw user data from CSV file
    columns:
      - name: id
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
        description: User registration date
        tests:
          - not_null

models:
  - name: my_first_model
    description: Cleaned and transformed user data
    columns:
      - name: id
        description: User ID
        tests:
          - unique
          - not_null
      - name: full_name_upper
        description: Full name in uppercase
        tests:
          - not_null
      - name: days_since_registration
        description: Number of days since user registration
```

### 6. Run Your Pipeline
```bash
# Test database connection
dbt debug

# Load CSV data into DuckDB
dbt seed

# Run your models
dbt run

# Run tests
dbt test

# View your data
dbt show --select my_first_model
```

**What happened?**
1. `dbt seed` loaded your CSV into DuckDB as a table
2. `dbt run` created your transformed model
3. `dbt test` verified data quality
4. `dbt show` displays the results

## Materializations

Control how dbt builds your models in the warehouse:

### Table
```sql
{{ config(materialized='table') }}

SELECT * FROM {{ ref('raw_orders') }}
```

### View
```sql
{{ config(materialized='view') }}

SELECT * FROM {{ ref('base_orders') }}
```

### Incremental
```sql
{{ config(materialized='incremental') }}

SELECT * FROM {{ ref('raw_events') }}
{% if is_incremental() %}
  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
```

### Ephemeral
```sql
{{ config(materialized='ephemeral') }}

SELECT * FROM {{ ref('raw_users') }}
WHERE created_at IS NOT NULL
```

## Jinja Templating

dbt uses Jinja for dynamic SQL generation:

### Variables
```sql
SELECT * FROM orders
WHERE order_date >= '{{ var("start_date") }}'
```

### Conditionals
```sql
SELECT 
    order_id,
    {% if target.name == 'prod' %}
        customer_id
    {% else %}
        'masked' as customer_id
    {% endif %}
FROM orders
```

### Loops
```sql
SELECT 
    order_id,
    {% for status in ['pending', 'shipped', 'delivered'] %}
        SUM(CASE WHEN status = '{{ status }}' THEN 1 ELSE 0 END) as {{ status }}_orders
        {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
FROM orders
GROUP BY order_id
```

## Testing

### Schema Tests (YAML)
```yaml
models:
  - name: customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - unique
      - name: status
        tests:
          - accepted_values:
              values: ['active', 'inactive', 'pending']
```

### Custom Data Tests
Create `tests/assert_positive_revenue.sql`:

```sql
-- tests/assert_positive_revenue.sql
SELECT *
FROM {{ ref('orders') }}
WHERE total_amount <= 0
```

### Run Tests
```bash
# Run all tests
dbt test

# Run specific tests
dbt test --models my_first_model
```

## Documentation

### Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

### Add Descriptions
```yaml
models:
  - name: customers
    description: "Customer dimension table with current status"
    columns:
      - name: customer_id
        description: "Unique identifier for each customer"
      - name: lifetime_value
        description: "Total revenue generated by customer (USD)"
```

### Add Model Documentation
Create `models/customers.md`:

```markdown
{% docs customers_table %}
This table contains customer information aggregated from multiple sources.

## Data Sources
- Application database users table
- Payment processor customer data
- Support ticket customer information

## Update Frequency
Updated nightly at 2 AM UTC via scheduled dbt Cloud job.
{% enddocs %}
```

Reference in schema.yml:
```yaml
models:
  - name: customers
    description: "{{ doc('customers_table') }}"
```

## Best Practices

### 1. Folder Structure
```
models/
├── staging/     # 1:1 with source tables, basic cleaning
├── intermediate/  # Business logic, not end-user facing
└── marts/       # Final tables for analysis
```

### 2. Naming Conventions
- **Sources**: `src_<source>_<table>`
- **Staging**: `stg_<source>_<table>`
- **Intermediate**: `int_<business_concept>`
- **Facts**: `fct_<business_process>`
- **Dimensions**: `dim_<business_entity>`

### 3. Model Organization
```sql
-- staging/stg_orders.sql
WITH source_data AS (
    SELECT * FROM {{ ref('raw_orders') }}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        CAST(order_date AS DATE) as order_date,
        CAST(total_amount AS DECIMAL(10,2)) as total_amount
    FROM source_data
    WHERE order_date IS NOT NULL
)

SELECT * FROM cleaned
```

### 4. Use ref() and source()
```sql
-- Good
SELECT * FROM {{ ref('stg_customers') }}
JOIN {{ ref('raw_orders') }} USING (customer_id)

-- Bad
SELECT * FROM stg_customers
JOIN raw_orders USING (customer_id)
```

### 5. DRY Principle with Macros
Create `macros/get_payment_methods.sql`:

```sql
{% macro get_payment_methods() %}
    CASE 
        WHEN payment_method = 'cc' THEN 'credit_card'
        WHEN payment_method = 'pp' THEN 'paypal'
        ELSE 'other'
    END
{% endmacro %}
```

Use in models:
```sql
SELECT 
    order_id,
    {{ get_payment_methods() }} as payment_method_clean
FROM {{ ref('stg_orders') }}
```

## Next Steps

1. **Set up version control** with Git
2. **Implement CI/CD** with dbt Cloud or GitHub Actions
3. **Add more advanced testing** with custom tests and packages
4. **Use packages** from the dbt Hub for common transformations
5. **Implement data lineage** and impact analysis
6. **Set up monitoring** and alerting for data quality

## Common Commands

```bash
# Development workflow
dbt compile          # Compile without running
dbt run             # Run models
dbt test            # Run tests
dbt docs generate   # Generate documentation
dbt docs serve      # Serve documentation locally

# Specific selections
dbt run --models my_model           # Run specific model
dbt run --models +my_model          # Run model and upstream
dbt run --models my_model+          # Run model and downstream
dbt run --models tag:staging        # Run models with tag
dbt run --exclude tag:slow          # Exclude models with tag

# Data management
dbt seed            # Load seed files
dbt snapshot        # Run snapshots
dbt clean           # Remove target directory
```

This tutorial provides a solid foundation for understanding and using dbt in data engineering workflows. Practice with the realistic use case example to reinforce these concepts!