# dbt Learning Repository

A comprehensive learning repository for mastering dbt (Data Build Tool) with practical examples, tutorials, and end-to-end data pipeline implementations.

## 🎯 Overview

This repository demonstrates professional data transformation workflows using dbt with DuckDB, covering everything from basic concepts to advanced analytics and machine learning feature engineering.

## 🛠 Tech Stack

- ✅ **dbt** - Data transformation framework
- ✅ **DuckDB** - Embedded analytical database
- ✅ **Python 3.12** - Runtime environment
- ✅ **uv** - Fast Python package manager

## 📚 Learning Materials

### 1. Basic Tutorial
**File**: `dbt_tutorial.md`
- ✅ Introduction to dbt concepts
- ✅ Setting up your first project
- ✅ Basic model creation
- ✅ Testing and documentation

### 2. E-commerce Pipeline Example
**File**: `ecommerce_pipeline_example.md`
- ✅ Practical e-commerce use case
- ✅ Data modeling patterns
- ✅ Dimensional modeling
- ✅ Business logic implementation

### 3. E-commerce TDD Approach
**File**: `ecommerce_pipeline_tdd.md`
- ✅ Test-Driven Development with dbt
- ✅ Data quality frameworks
- ✅ Testing strategies
- ✅ CI/CD integration

### 4. **End-to-End Analytics Workflow** ⭐
**File**: `ecommerce_analytics_end_to_end.md`

Comprehensive tutorial demonstrating the complete data analytics lifecycle:

#### 📊 Modules Covered

1. **Data Wrangling**
   - Hierarchical indexing patterns
   - Data cleaning and standardization
   - Combining datasets (merge, concat, join)
   - Reshaping and pivoting data

2. **Data Aggregation**
   - Split-Apply-Combine methodology
   - RFM (Recency, Frequency, Monetary) analysis
   - Pivot tables and cross-tabulation
   - Customer behavior metrics

3. **Exploratory Data Analysis (EDA)**
   - Distribution analysis
   - Correlation analysis
   - Customer segmentation visualization
   - Product performance analysis

4. **Predictive Modeling Preparation**
   - Feature engineering for ML
   - Customer churn prediction features
   - Customer Lifetime Value (CLV) modeling
   - Training data preparation

#### 🎓 What You'll Build

- ✅ **Staging Layer**: Clean, standardized data from raw sources
- ✅ **Intermediate Models**: RFM analysis, customer orders aggregation
- ✅ **Data Marts**:
  - Customer metrics with CLV predictions
  - Customer segmentation (Champions, At Risk, etc.)
  - Product performance analytics
- ✅ **ML Features**: Ready-to-use features for:
  - Churn prediction models
  - CLV forecasting
  - Customer behavior analysis

#### 💼 Business Use Cases

1. **Churn Prevention Campaigns**
   - Identify high-value at-risk customers
   - Targeted win-back strategies

2. **Marketing Budget Allocation**
   - CLV-based budget distribution
   - Segment-specific campaign planning

3. **Product Recommendations**
   - Customer-product affinity matrices
   - Collaborative filtering data prep

## 🚀 Quick Start

### Prerequisites

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Python 3.12
uv python install 3.12
```

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd dbt_get_started

# Install dependencies
uv sync

# Navigate to the project
cd my_first_project

# Load seed data
dbt seed

# Run all models
dbt run

# Run tests
dbt test

# Generate and view documentation
dbt docs generate
dbt docs serve
```

## 🧪 Seed Data Options

You can load the provided sample CSVs in two ways:

1. Configure dbt to include the root-level `data_seeds/` directory:

```yaml
# my_first_project/dbt_project.yml
seed-paths: ["seeds", "../data_seeds"]
```

2. Or copy the CSVs into the project's default seed folder:

```bash
cp -R data_seeds my_first_project/seeds/raw
```

Then run:

```bash
cd my_first_project
dbt seed
```

## 📁 Project Structure

```
dbt_get_started/
├── data_seeds/                 # Sample CSVs for seeding
│   ├── raw_customers.csv
│   ├── raw_orders.csv
│   ├── raw_order_items.csv
│   └── raw_products.csv
├── my_first_project/              # Main dbt project
│   ├── models/
│   │   ├── staging/               # Raw data cleaning & standardization
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   └── stg_products.sql
│   │   ├── intermediate/          # Business logic & aggregations
│   │   │   ├── int_customer_orders.sql
│   │   │   ├── int_customer_rfm.sql
│   │   │   └── int_product_metrics.sql
│   │   ├── marts/                 # Analytics-ready tables
│   │   │   ├── fct_customer_metrics.sql
│   │   │   ├── dim_customer_segments.sql
│   │   │   └── fct_product_performance.sql
│   │   └── ml/                    # ML feature engineering
│   │       └── ml_customer_features.sql
│   ├── seeds/                     # Default seed folder (configure to include ../data_seeds)
│   │   └── raw/
│   ├── tests/                     # Custom data quality tests
│   ├── dbt_project.yml           # Project configuration
│   └── my_dbt.duckdb             # DuckDB database
├── dbt_tutorial.md               # Basic tutorial
├── ecommerce_pipeline_example.md # E-commerce pipeline guide
├── ecommerce_pipeline_tdd.md     # TDD approach guide
├── ecommerce_analytics_end_to_end.md  # ⭐ Complete analytics workflow
├── pyproject.toml                # Python dependencies
└── README.md                     # This file
```

## 🔍 Key Concepts Demonstrated
  - unique
  - not_null
  - accepted_values
  - relationships
```

### Custom Tests
```sql
# tests/assert_positive_clv.sql
select *
from {{ ref('fct_customer_metrics') }}
where predicted_clv_12m < 0
```

### Running Tests
```bash
# All tests
dbt test

# Specific model
dbt test --select fct_customer_metrics

# By tag
dbt test --select tag:customer
```

## 📊 Example Queries

### Customer Segmentation
```sql
select
    customer_segment,
    count(*) as customers,
    avg(monetary_value) as avg_value,
    avg(predicted_clv_12m) as avg_clv
from {{ ref('dim_customer_segments') }}
group by 1
order by avg_clv desc;
```

### Churn Risk Analysis
```sql
select
    churn_risk,
    customer_segment,
    count(*) as at_risk_customers,
    sum(monetary_value) as revenue_at_risk
from {{ ref('fct_customer_metrics') }}
where churn_risk in ('High', 'Medium')
group by 1, 2;
```

### Product Performance
```sql
select
    performance_tier,
    category,
    count(*) as product_count,
    sum(revenue) as total_revenue,
    sum(total_profit) as total_profit
from {{ ref('fct_product_performance') }}
group by 1, 2;
```

## 🎯 Best Practices

1. **Modularity**: Keep models focused and reusable
2. **Documentation**: Document all models in schema.yml
3. **Testing**: Test at every layer (staging → marts)
4. **Naming Conventions**:
   - `stg_` for staging models
   - `int_` for intermediate models
   - `fct_` for fact tables
   - `dim_` for dimension tables
   - `ml_` for ML features
5. **Materialization**:
   - Views for staging (fast, always fresh)
   - Tables for marts (performance)
   - Incremental for large datasets

## 🔧 Common Commands

```bash
# Development workflow
dbt compile                    # Check SQL syntax
dbt run --select model_name    # Run specific model
dbt run --select +model_name   # Run model + upstream
dbt run --select model_name+   # Run model + downstream

# By tags
dbt run --select tag:staging
dbt run --select tag:ml

# Testing
dbt test
dbt test --select model_name

# Documentation
dbt docs generate
dbt docs serve --port 8080

# Full refresh
dbt run --full-refresh

# Production
dbt run --target prod
```

## 🚢 Deployment Considerations

### For Production

1. **Orchestration**: Schedule with Airflow/Dagster/Prefect
2. **CI/CD**: Automate testing on PR
3. **Monitoring**: Set up data quality alerts
4. **Incremental Models**: Use for large datasets
5. **Performance**: Optimize materializations

### Example Airflow DAG
```python
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG('dbt_ecommerce_analytics', schedule_interval='@daily')

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /path/to/project && dbt run',
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /path/to/project && dbt test',
    dag=dag
)

dbt_run >> dbt_test
```

## 📈 Performance Tuning

- ✅ Use `{{ ref() }}` for model dependencies
- ✅ Leverage incremental models for large tables
- ✅ Optimize with appropriate materializations
- ✅ Use CTEs for readability and performance
- ✅ Index columns used in joins (database-specific)

## 🤝 Contributing

Feel free to contribute by:
- ✅ Adding new examples
- ✅ Improving documentation
- ✅ Reporting issues
- ✅ Suggesting best practices

## 📝 License

This is a learning repository for educational purposes.

## 🔗 Resources

- ✅ [dbt Documentation](https://docs.getdbt.com/)
- ✅ [dbt Discourse Community](https://discourse.getdbt.com/)
- ✅ [DuckDB Documentation](https://duckdb.org/docs/)
- ✅ [Analytics Engineering Guide](https://www.getdbt.com/analytics-engineering/)

## 💡 What's Next?

After completing these tutorials, you'll be ready to:
- ✅ Build production data pipelines
- ✅ Implement data quality frameworks
- ✅ Create analytics-ready data models
- ✅ Prepare features for ML models
- ✅ Apply analytics engineering best practices

Happy learning! 🎉
