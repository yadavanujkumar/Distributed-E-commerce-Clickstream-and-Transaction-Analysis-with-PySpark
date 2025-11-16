#!/usr/bin/env python3
"""
End-to-End Workflow Runner
Executes the complete analytics pipeline from data generation to recommendations
"""

import sys
import os
import time

def print_section(title):
    """Print formatted section header"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70 + "\n")

def run_data_generation():
    """Step 1: Generate sample data"""
    print_section("STEP 1: Generate Sample Data")
    
    import generate_data
    generate_data.main()
    
    print("\n✓ Data generation completed")
    return True

def run_analytics_pipeline():
    """Step 2: Run the analytics pipeline"""
    print_section("STEP 2: Run Analytics Pipeline")
    
    # Import and run the analytics
    import ecommerce_analytics
    
    analytics = ecommerce_analytics.EcommerceAnalytics()
    analytics.run_complete_pipeline()
    
    print("\n✓ Analytics pipeline completed")
    return True

def verify_outputs():
    """Step 3: Verify all outputs"""
    print_section("STEP 3: Verify Outputs")
    
    # Check data files
    data_files = ["data/clicks.csv", "data/transactions.json", "data/products.csv"]
    print("Checking data files:")
    for file in data_files:
        if os.path.exists(file):
            size_mb = os.path.getsize(file) / (1024 * 1024)
            print(f"  ✓ {file} ({size_mb:.2f} MB)")
        else:
            print(f"  ✗ {file} not found")
            return False
    
    # Check output files
    output_dirs = ["output/clicks_enriched.parquet", "output/transactions_enriched.parquet"]
    print("\nChecking output files:")
    for dir_path in output_dirs:
        if os.path.exists(dir_path):
            print(f"  ✓ {dir_path}")
        else:
            print(f"  ✗ {dir_path} not found")
            return False
    
    print("\n✓ All outputs verified")
    return True

def main():
    """Main workflow execution"""
    print("="*70)
    print("  PySpark E-commerce Analytics - Complete Workflow")
    print("="*70)
    print("\nThis script will:")
    print("  1. Generate sample e-commerce data")
    print("  2. Run ETL, analytics, and ML pipeline")
    print("  3. Verify all outputs")
    print("\nEstimated time: 2-3 minutes")
    print()
    
    input("Press Enter to start...")
    
    start_time = time.time()
    
    try:
        # Run all steps
        if not run_data_generation():
            print("\n✗ Data generation failed")
            return 1
        
        if not run_analytics_pipeline():
            print("\n✗ Analytics pipeline failed")
            return 1
        
        if not verify_outputs():
            print("\n✗ Output verification failed")
            return 1
        
        # Success!
        elapsed_time = time.time() - start_time
        
        print_section("WORKFLOW COMPLETED SUCCESSFULLY!")
        print(f"Total execution time: {elapsed_time:.2f} seconds\n")
        print("What was accomplished:")
        print("  ✓ Generated 50,000 clickstream records")
        print("  ✓ Generated 10,000 transaction records")
        print("  ✓ Generated 200 product records")
        print("  ✓ Cleaned and enriched data")
        print("  ✓ Calculated 4 business KPIs")
        print("  ✓ Trained ALS recommendation model")
        print("  ✓ Generated product recommendations")
        print("  ✓ Saved data to Parquet format")
        print("\nNext steps:")
        print("  - Review the KPI outputs above")
        print("  - Explore the Parquet files in output/")
        print("  - Modify parameters in the scripts")
        print("  - Run tests: python test_analytics.py")
        print()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n✗ Workflow interrupted by user")
        return 1
    except Exception as e:
        print(f"\n\n✗ Workflow failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
