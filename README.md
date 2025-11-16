# Distributed E-commerce Clickstream and Transaction Analysis with PySpark

A comprehensive Big Data solution for analyzing e-commerce clickstream and transaction data using Apache Spark. This project implements ETL pipelines, business intelligence analytics, and a collaborative filtering recommendation engine.

## Overview

This project analyzes massive datasets of simulated e-commerce activity to derive business intelligence (KPIs) and build a recommendation engine. It demonstrates the use of PySpark for distributed data processing, Spark SQL for analytics, and MLlib for machine learning.

## Features

### Phase 1: Data Ingestion and ETL
- Ingests data from multiple sources (CSV, JSON)
- Optimizes data types and handles missing values
- Joins and enriches datasets
- Outputs cleaned data in Parquet format for optimized querying

### Phase 2: Business Intelligence & Analytics
- **Top Products**: Identifies the top 5 most frequently purchased products
- **Revenue Analysis**: Calculates total revenue per day for the last 30 days
- **Popular Categories**: Determines the top 3 product categories by page views
- **Funnel Analysis**: Calculates conversion rates from views to purchases

### Phase 3: Recommendation Engine
- Implements collaborative filtering using ALS (Alternating Least Squares)
- Trains and evaluates model using RMSE metric
- Generates personalized product recommendations

## Technical Requirements Met

- ✅ Uses PySpark for all data processing
- ✅ Demonstrates DataFrame transformations (withColumn, join, filter, groupBy)
- ✅ Uses Spark SQL for querying (spark.sql())
- ✅ Implements PySpark MLlib (ALS algorithm)
- ✅ Tunes Spark configuration (spark.sql.shuffle.partitions set to 8)

## Project Structure

```
.
├── generate_data.py          # Script to generate simulated datasets
├── ecommerce_analytics.py    # Main PySpark analytics pipeline
├── requirements.txt          # Python dependencies
├── data/                     # Directory for input datasets (generated)
│   ├── clicks.csv
│   ├── transactions.json
│   └── products.csv
└── output/                   # Directory for processed data (generated)
    ├── clicks_enriched.parquet
    └── transactions_enriched.parquet
```

## Installation

### Prerequisites
- Python 3.8 or higher
- Java 8 or 11 (required for PySpark)
- Apache Spark 3.5.0

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yadavanujkumar/Distributed-E-commerce-Clickstream-and-Transaction-Analysis-with-PySpark.git
cd Distributed-E-commerce-Clickstream-and-Transaction-Analysis-with-PySpark
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Step 1: Generate Sample Data

First, generate the simulated datasets:

```bash
python generate_data.py
```

This creates three datasets in the `data/` directory:
- `clicks.csv`: 50,000 user clickstream records
- `transactions.json`: 10,000 transaction records
- `products.csv`: 200 product records

### Step 2: Run the Analytics Pipeline

Execute the complete PySpark pipeline:

```bash
python ecommerce_analytics.py
```

The pipeline will:
1. Ingest and clean the data
2. Perform ETL transformations
3. Calculate business KPIs
4. Train a recommendation model
5. Generate product recommendations

## Configuration

### Spark Configuration
The project is configured with optimized Spark settings:

```python
spark.sql.shuffle.partitions = 8
```

**Justification**: Reduced from the default 200 partitions to 8 for better performance on small to medium datasets, minimizing shuffle overhead.

### Data Generation Parameters
You can customize the data generation in `generate_data.py`:

```python
NUM_USERS = 1000          # Number of unique users
NUM_PRODUCTS = 200        # Number of unique products
NUM_CLICKS = 50000        # Number of click records
NUM_TRANSACTIONS = 10000  # Number of transaction records
```

## Output

### KPI Reports
The pipeline generates the following analytics:

1. **Top 5 Products by Quantity Sold**
2. **Daily Revenue for Last 30 Days**
3. **Top 3 Categories by Page Views**
4. **Conversion Rate Analysis for Top 10 Viewed Products**

### Recommendation Results
- RMSE evaluation metric for model performance
- Top 5 product recommendations for 10 users

### Data Output
Processed data is saved in Parquet format:
- `output/clicks_enriched.parquet`
- `output/transactions_enriched.parquet`

## Technical Details

### Data Cleaning
- Removes null user IDs from clickstream data
- Filters out transactions with non-positive prices or quantities
- Handles missing values appropriately

### Join Operations
- Inner joins between clicks/transactions and products
- Enriches data with product categories, names, and features

### ML Model
- **Algorithm**: Alternating Least Squares (ALS)
- **Iterations**: 10
- **Regularization**: 0.1
- **Cold Start Strategy**: Drop
- **Train/Test Split**: 80/20

## Performance Considerations

- Uses Parquet format for columnar storage and fast querying
- Optimizes shuffle partitions based on data size
- Caches frequently accessed DataFrames where appropriate
- Uses broadcast joins for small dimension tables

## Future Enhancements

- Add support for streaming data ingestion
- Implement additional recommendation algorithms
- Add data quality validation checks
- Create visualization dashboard for KPIs
- Implement A/B testing framework

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Author

Yadav Anuj Kumar

## Acknowledgments

- Apache Spark for distributed computing framework
- PySpark for Python API
- MLlib for machine learning capabilities