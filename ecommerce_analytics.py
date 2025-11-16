"""
Distributed E-commerce Clickstream and Transaction Analysis with PySpark
Implements ETL, Business Intelligence, and Recommendation Engine
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, count, sum as spark_sum, 
    desc, date_format, datediff, current_date, lit
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import os

# Configuration
DATA_DIR = "data"
OUTPUT_DIR = "output"
PARQUET_OUTPUT = f"{OUTPUT_DIR}/cleaned_data.parquet"

class EcommerceAnalytics:
    """Main class for e-commerce analytics using PySpark"""
    
    def __init__(self):
        """Initialize Spark session with optimized configuration"""
        self.spark = SparkSession.builder \
            .appName("E-commerce Clickstream and Transaction Analysis") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        # Justification: Setting shuffle partitions to 8 for better performance
        # on small to medium datasets, reducing overhead from default 200 partitions
        
        print("Spark session initialized successfully")
        print(f"Spark version: {self.spark.version}")
    
    # ========== PHASE 1: DATA INGESTION AND ETL ==========
    
    def ingest_data(self):
        """Ingest data from CSV and JSON files into DataFrames"""
        print("\n=== Phase 1: Data Ingestion and ETL ===")
        
        # Ingest clicks.csv
        print("Ingesting clicks.csv...")
        clicks_df = self.spark.read.csv(
            f"{DATA_DIR}/clicks.csv",
            header=True,
            inferSchema=True
        )
        
        # Cast timestamp and optimize types
        clicks_df = clicks_df.withColumn(
            "timestamp", 
            to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        ).withColumn(
            "page_views", 
            col("page_views").cast(IntegerType())
        )
        
        print(f"Clicks loaded: {clicks_df.count()} records")
        
        # Ingest transactions.json
        print("Ingesting transactions.json...")
        transactions_df = self.spark.read.json(f"{DATA_DIR}/transactions.json")
        
        # Cast timestamp and optimize types
        transactions_df = transactions_df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        ).withColumn(
            "price",
            col("price").cast(DoubleType())
        ).withColumn(
            "quantity",
            col("quantity").cast(IntegerType())
        )
        
        print(f"Transactions loaded: {transactions_df.count()} records")
        
        # Ingest products.csv
        print("Ingesting products.csv...")
        products_df = self.spark.read.csv(
            f"{DATA_DIR}/products.csv",
            header=True,
            inferSchema=True
        )
        
        print(f"Products loaded: {products_df.count()} records")
        
        return clicks_df, transactions_df, products_df
    
    def clean_and_transform(self, clicks_df, transactions_df, products_df):
        """Clean and transform the data"""
        print("\nCleaning and transforming data...")
        
        # Handle missing values - drop records with null user_ids in clicks
        clicks_clean = clicks_df.filter(col("user_id").isNotNull())
        print(f"Clicks after removing null user_ids: {clicks_clean.count()} records")
        
        # Filter out invalid transactions (non-positive price or quantity)
        transactions_clean = transactions_df.filter(
            (col("price") > 0) & (col("quantity") > 0)
        )
        print(f"Transactions after filtering invalid records: {transactions_clean.count()} records")
        
        # Join clicks with products to enrich data
        clicks_enriched = clicks_clean.join(
            products_df,
            clicks_clean.product_id == products_df.product_id,
            "inner"
        ).select(
            clicks_clean.user_id,
            clicks_clean.product_id,
            clicks_clean.timestamp.alias("click_timestamp"),
            clicks_clean.page_views,
            products_df.category,
            products_df.name.alias("product_name"),
            products_df.features
        )
        
        print(f"Enriched clicks: {clicks_enriched.count()} records")
        
        # Join transactions with products to enrich data
        transactions_enriched = transactions_clean.join(
            products_df,
            transactions_clean.product_id == products_df.product_id,
            "inner"
        ).select(
            transactions_clean.user_id,
            transactions_clean.product_id,
            transactions_clean.price,
            transactions_clean.quantity,
            transactions_clean.timestamp.alias("transaction_timestamp"),
            products_df.category,
            products_df.name.alias("product_name"),
            products_df.features
        )
        
        print(f"Enriched transactions: {transactions_enriched.count()} records")
        
        return clicks_enriched, transactions_enriched
    
    def save_to_parquet(self, clicks_df, transactions_df):
        """Save cleaned and joined datasets to Parquet format"""
        print("\nSaving data to Parquet format...")
        
        # Save clicks
        clicks_output = f"{OUTPUT_DIR}/clicks_enriched.parquet"
        clicks_df.write.mode("overwrite").parquet(clicks_output)
        print(f"Clicks saved to {clicks_output}")
        
        # Save transactions
        transactions_output = f"{OUTPUT_DIR}/transactions_enriched.parquet"
        transactions_df.write.mode("overwrite").parquet(transactions_output)
        print(f"Transactions saved to {transactions_output}")
        
        return clicks_output, transactions_output
    
    # ========== PHASE 2: BUSINESS INTELLIGENCE & ANALYTICS ==========
    
    def load_parquet_data(self, clicks_path, transactions_path):
        """Load data from Parquet files"""
        print("\n=== Phase 2: Business Intelligence & Analytics ===")
        clicks_df = self.spark.read.parquet(clicks_path)
        transactions_df = self.spark.read.parquet(transactions_path)
        
        # Register as temp views for Spark SQL
        clicks_df.createOrReplaceTempView("clicks")
        transactions_df.createOrReplaceTempView("transactions")
        
        return clicks_df, transactions_df
    
    def kpi_top_products(self):
        """KPI 1: Top 5 most frequently purchased products"""
        print("\n--- KPI 1: Top 5 Most Frequently Purchased Products ---")
        
        query = """
        SELECT 
            product_id,
            product_name,
            category,
            SUM(quantity) as total_quantity
        FROM transactions
        GROUP BY product_id, product_name, category
        ORDER BY total_quantity DESC
        LIMIT 5
        """
        
        result = self.spark.sql(query)
        result.show(truncate=False)
        return result
    
    def kpi_revenue_analysis(self):
        """KPI 2: Total revenue per day for last 30 days"""
        print("\n--- KPI 2: Total Revenue Per Day (Last 30 Days) ---")
        
        query = """
        SELECT 
            DATE(transaction_timestamp) as date,
            ROUND(SUM(price * quantity), 2) as total_revenue
        FROM transactions
        WHERE transaction_timestamp >= DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY DATE(transaction_timestamp)
        ORDER BY date DESC
        """
        
        result = self.spark.sql(query)
        result.show(30, truncate=False)
        return result
    
    def kpi_popular_categories(self):
        """KPI 3: Top 3 categories with highest page views"""
        print("\n--- KPI 3: Top 3 Categories with Highest Page Views ---")
        
        query = """
        SELECT 
            category,
            SUM(page_views) as total_page_views
        FROM clicks
        GROUP BY category
        ORDER BY total_page_views DESC
        LIMIT 3
        """
        
        result = self.spark.sql(query)
        result.show(truncate=False)
        return result
    
    def kpi_funnel_analysis(self):
        """KPI 4: Conversion rate from view to purchase for top 10 viewed products"""
        print("\n--- KPI 4: Conversion Rate Funnel Analysis (Top 10 Viewed Products) ---")
        
        # First, get top 10 most viewed products
        top_viewed_query = """
        SELECT 
            product_id,
            product_name,
            SUM(page_views) as total_views
        FROM clicks
        GROUP BY product_id, product_name
        ORDER BY total_views DESC
        LIMIT 10
        """
        
        top_viewed = self.spark.sql(top_viewed_query)
        top_viewed.createOrReplaceTempView("top_viewed_products")
        
        # Calculate conversion rate
        conversion_query = """
        SELECT 
            tv.product_id,
            tv.product_name,
            tv.total_views,
            COALESCE(SUM(t.quantity), 0) as total_purchases,
            ROUND(
                COALESCE(SUM(t.quantity), 0) * 100.0 / tv.total_views, 
                2
            ) as conversion_rate_percent
        FROM top_viewed_products tv
        LEFT JOIN transactions t ON tv.product_id = t.product_id
        GROUP BY tv.product_id, tv.product_name, tv.total_views
        ORDER BY conversion_rate_percent DESC
        """
        
        result = self.spark.sql(conversion_query)
        result.show(truncate=False)
        return result
    
    # ========== PHASE 3: RECOMMENDATION ENGINE ==========
    
    def build_recommendation_model(self, transactions_df):
        """Build ALS recommendation model"""
        print("\n=== Phase 3: Recommendation Engine (MLlib) ===")
        
        # Prepare training data
        print("Preparing training data...")
        training_data = transactions_df.select(
            col("user_id").cast(IntegerType()),
            col("product_id").cast(IntegerType()),
            col("quantity").cast(IntegerType()).alias("rating")
        )
        
        print(f"Training data size: {training_data.count()} records")
        
        # Split data into training and testing sets (80-20 split)
        print("Splitting data into training and testing sets...")
        train_data, test_data = training_data.randomSplit([0.8, 0.2], seed=42)
        
        print(f"Training set: {train_data.count()} records")
        print(f"Testing set: {test_data.count()} records")
        
        # Train ALS model
        print("\nTraining ALS model...")
        als = ALS(
            maxIter=10,
            regParam=0.1,
            userCol="user_id",
            itemCol="product_id",
            ratingCol="rating",
            coldStartStrategy="drop",
            seed=42
        )
        
        model = als.fit(train_data)
        print("Model training completed")
        
        # Evaluate model
        print("\nEvaluating model...")
        predictions = model.transform(test_data)
        
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        
        rmse = evaluator.evaluate(predictions)
        print(f"Root Mean Square Error (RMSE): {rmse:.4f}")
        
        return model, training_data
    
    def generate_recommendations(self, model, num_users=10, num_recommendations=5):
        """Generate product recommendations for specific users"""
        print(f"\n--- Generating Top {num_recommendations} Product Recommendations for {num_users} Users ---")
        
        # Generate recommendations for all users
        user_recs = model.recommendForAllUsers(num_recommendations)
        
        # Show recommendations for first num_users
        result = user_recs.limit(num_users)
        result.show(num_users, truncate=False)
        
        # Also show in a more readable format
        print("\n--- Detailed Recommendations ---")
        for row in result.collect():
            print(f"\nUser ID: {row.user_id}")
            print("Recommended Products:")
            for rec in row.recommendations:
                print(f"  - Product ID: {rec.product_id}, Predicted Rating: {rec.rating:.2f}")
        
        return result
    
    def run_complete_pipeline(self):
        """Run the complete analytics pipeline"""
        try:
            # Phase 1: ETL
            clicks_df, transactions_df, products_df = self.ingest_data()
            clicks_clean, transactions_clean = self.clean_and_transform(
                clicks_df, transactions_df, products_df
            )
            clicks_path, transactions_path = self.save_to_parquet(
                clicks_clean, transactions_clean
            )
            
            # Phase 2: Analytics
            clicks_df, transactions_df = self.load_parquet_data(
                clicks_path, transactions_path
            )
            self.kpi_top_products()
            self.kpi_revenue_analysis()
            self.kpi_popular_categories()
            self.kpi_funnel_analysis()
            
            # Phase 3: Recommendations
            model, training_data = self.build_recommendation_model(transactions_df)
            self.generate_recommendations(model)
            
            print("\n" + "="*60)
            print("Pipeline execution completed successfully!")
            print("="*60)
            
        except Exception as e:
            print(f"\nError during pipeline execution: {e}")
            raise
        finally:
            self.spark.stop()
            print("\nSpark session stopped")

def main():
    """Main execution function"""
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Initialize and run analytics
    analytics = EcommerceAnalytics()
    analytics.run_complete_pipeline()

if __name__ == "__main__":
    main()
