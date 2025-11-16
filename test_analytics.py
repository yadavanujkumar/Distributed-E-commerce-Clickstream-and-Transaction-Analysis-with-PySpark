"""
Test script to verify the PySpark e-commerce analytics implementation
This script validates the main functionality without running the full pipeline
"""

import os
import sys

def test_data_generation():
    """Test that data generation script creates required files"""
    print("Test 1: Data Generation")
    
    # Clean up any existing data
    import shutil
    if os.path.exists("data"):
        shutil.rmtree("data")
    
    # Import and run data generation
    import generate_data
    generate_data.main()
    
    # Verify files exist
    assert os.path.exists("data/clicks.csv"), "clicks.csv not created"
    assert os.path.exists("data/transactions.json"), "transactions.json not created"
    assert os.path.exists("data/products.csv"), "products.csv not created"
    
    # Verify files have content
    assert os.path.getsize("data/clicks.csv") > 1000, "clicks.csv appears empty"
    assert os.path.getsize("data/transactions.json") > 1000, "transactions.json appears empty"
    assert os.path.getsize("data/products.csv") > 1000, "products.csv appears empty"
    
    print("✓ Data generation test passed")
    return True

def test_imports():
    """Test that all required imports work"""
    print("\nTest 2: Import Dependencies")
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, to_timestamp
        from pyspark.ml.recommendation import ALS
        from pyspark.ml.evaluation import RegressionEvaluator
        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def test_spark_session():
    """Test that Spark session can be created"""
    print("\nTest 3: Spark Session Creation")
    
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("Test") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    assert spark is not None, "Failed to create Spark session"
    assert spark.version, "Spark version not available"
    
    print(f"✓ Spark session created (version {spark.version})")
    spark.stop()
    return True

def test_data_ingestion():
    """Test that data can be ingested into DataFrames"""
    print("\nTest 4: Data Ingestion")
    
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("Test") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    try:
        # Test CSV ingestion
        clicks_df = spark.read.csv("data/clicks.csv", header=True, inferSchema=True)
        clicks_count = clicks_df.count()
        assert clicks_count > 0, "No clicks data loaded"
        print(f"  - Loaded {clicks_count} clicks records")
        
        # Test JSON ingestion
        transactions_df = spark.read.json("data/transactions.json")
        trans_count = transactions_df.count()
        assert trans_count > 0, "No transactions data loaded"
        print(f"  - Loaded {trans_count} transactions records")
        
        # Test products CSV
        products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)
        prod_count = products_df.count()
        assert prod_count > 0, "No products data loaded"
        print(f"  - Loaded {prod_count} products records")
        
        print("✓ Data ingestion test passed")
        return True
    finally:
        spark.stop()

def test_transformations():
    """Test basic DataFrame transformations"""
    print("\nTest 5: DataFrame Transformations")
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, to_timestamp
    
    spark = SparkSession.builder \
        .appName("Test") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    try:
        clicks_df = spark.read.csv("data/clicks.csv", header=True, inferSchema=True)
        products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)
        
        # Test filtering null values
        clicks_clean = clicks_df.filter(col("user_id").isNotNull())
        assert clicks_clean.count() > 0, "Filter operation failed"
        print(f"  - Filter operation: {clicks_clean.count()} records after filtering")
        
        # Test join operation
        joined = clicks_clean.join(products_df, "product_id", "inner")
        assert joined.count() > 0, "Join operation failed"
        print(f"  - Join operation: {joined.count()} records after join")
        
        # Test column transformation
        transformed = clicks_clean.withColumn(
            "timestamp", 
            to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        )
        assert transformed.count() > 0, "Column transformation failed"
        print(f"  - Column transformation: successful")
        
        print("✓ DataFrame transformations test passed")
        return True
    finally:
        spark.stop()

def run_all_tests():
    """Run all tests"""
    print("=" * 60)
    print("PySpark E-commerce Analytics - Test Suite")
    print("=" * 60)
    
    tests = [
        test_data_generation,
        test_imports,
        test_spark_session,
        test_data_ingestion,
        test_transformations
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"✗ Test failed with exception: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return failed == 0

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
