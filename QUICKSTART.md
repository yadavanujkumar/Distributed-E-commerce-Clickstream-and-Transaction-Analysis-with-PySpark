# Quick Start Guide

## Installation

```bash
# Clone the repository
git clone https://github.com/yadavanujkumar/Distributed-E-commerce-Clickstream-and-Transaction-Analysis-with-PySpark.git
cd Distributed-E-commerce-Clickstream-and-Transaction-Analysis-with-PySpark

# Install dependencies
pip install -r requirements.txt
```

## Usage

### 1. Generate Sample Data

```bash
python generate_data.py
```

**Output**: Creates `data/` directory with:
- `clicks.csv` - 50,000 clickstream records
- `transactions.json` - 10,000 transaction records  
- `products.csv` - 200 product records

### 2. Run Analytics Pipeline

```bash
python ecommerce_analytics.py
```

**This will**:
1. Ingest and clean the data
2. Calculate 4 business KPIs:
   - Top 5 products by sales
   - Daily revenue (last 30 days)
   - Top 3 categories by page views
   - Conversion rates for top 10 products
3. Train ALS recommendation model
4. Generate product recommendations for 10 users
5. Save processed data to `output/` directory in Parquet format

### 3. Run Tests (Optional)

```bash
python test_analytics.py
```

**Validates**:
- Data generation works
- All dependencies are installed
- Spark session creation
- Data ingestion from CSV/JSON
- DataFrame transformations

## Expected Output

### KPI Reports
The pipeline will display tables showing:
- Most popular products
- Revenue trends
- Category performance
- Conversion funnel metrics

### Recommendation Engine
- RMSE score (typically ~1.5-2.0)
- Top 5 product recommendations per user
- Predicted ratings for each recommendation

### Data Files
Processed data saved in `output/`:
- `clicks_enriched.parquet/`
- `transactions_enriched.parquet/`

## Configuration

Edit these parameters in the scripts to customize:

### Data Generation (`generate_data.py`)
```python
NUM_USERS = 1000          # Number of users
NUM_PRODUCTS = 200        # Number of products
NUM_CLICKS = 50000        # Number of click records
NUM_TRANSACTIONS = 10000  # Number of transactions
```

### ALS Model (`ecommerce_analytics.py`)
```python
als = ALS(
    maxIter=10,           # Training iterations
    regParam=0.1,         # Regularization
    userCol="user_id",
    itemCol="product_id",
    ratingCol="rating"
)
```

### Spark Configuration (`ecommerce_analytics.py`)
```python
.config("spark.sql.shuffle.partitions", "8")  # Adjust based on data size
```

## Troubleshooting

### Issue: "ModuleNotFoundError: No module named 'pyspark'"
**Solution**: Install PySpark: `pip install pyspark`

### Issue: "ModuleNotFoundError: No module named 'numpy'"
**Solution**: Install numpy: `pip install numpy`

### Issue: Java not found
**Solution**: Install Java 8 or 11:
- Ubuntu/Debian: `sudo apt-get install openjdk-11-jdk`
- macOS: `brew install openjdk@11`
- Windows: Download from [Oracle](https://www.oracle.com/java/technologies/downloads/)

### Issue: OutOfMemoryError
**Solution**: Increase Spark memory:
```python
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")
```

## Performance Tips

1. **For larger datasets**: Increase shuffle partitions
   ```python
   .config("spark.sql.shuffle.partitions", "200")
   ```

2. **For smaller datasets**: Decrease shuffle partitions  
   ```python
   .config("spark.sql.shuffle.partitions", "4")
   ```

3. **Enable compression**: Already configured via Parquet format

4. **Cache frequently used DataFrames**: Add `.cache()` after loading

## Next Steps

- Explore the generated Parquet files using `spark.read.parquet()`
- Modify the SQL queries to analyze different metrics
- Tune ALS parameters for better recommendations
- Scale up data generation for larger datasets
- Add custom KPIs for your use case

## Documentation

- **README.md** - Overview and detailed usage instructions
- **IMPLEMENTATION.md** - Technical implementation details
- **test_analytics.py** - Test suite for validation

## Support

For issues or questions:
1. Check the documentation files
2. Review the error messages carefully
3. Ensure all dependencies are installed
4. Verify Java is properly configured
5. Check Spark logs for detailed error information
