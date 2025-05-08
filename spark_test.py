import os
from pyspark.sql import SparkSession

# Create a minimal Spark session for testing with reduced memory settings
def create_spark_session():
    print("Creating minimal Spark session with reduced memory settings...")
    return (SparkSession.builder
            .appName("Spark Test")
            .master("local[1]")  # Use only one thread
            .config("spark.driver.memory", "1g")  # Reduce driver memory
            .config("spark.executor.memory", "1g")  # Reduce executor memory
            .config("spark.driver.maxResultSize", "512m")  # Reduce result size
            .getOrCreate())

# Just test if Spark can read a CSV file
def test_spark_csv():
    try:
        print("Starting Spark test...")
        spark = create_spark_session()
        print("Spark session created successfully")
        
        # Show Spark version
        print(f"Spark version: {spark.version}")
        
        # Try reading a small file first
        print("Trying to read a simple CSV file...")
        # Create a test data frame
        test_data = [("1", "test")]
        columns = ["id", "name"]
        test_df = spark.createDataFrame(test_data, columns)
        test_df.show()
        
        print("Test dataframe created successfully")
        
        # If this works, then try to access the file system
        print("Files in current directory:")
        print(os.listdir("."))
        
        if "fake_data" in os.listdir("."):
            print("Files in fake_data directory:")
            print(os.listdir("fake_data"))
            
            # Try to read a CSV file
            print("Attempting to read CSV file...")
            file_path = "fake_data/Extraction_Cloture_Sinistre_Auto.csv"
            if os.path.exists(file_path):
                df = spark.read.option("header", True).csv(file_path)
                print(f"Successfully read CSV with {df.count()} rows")
                print(f"Columns: {', '.join(df.columns)}")
                
                # Show sample data
                print("Sample data:")
                df.show(5)
            else:
                print(f"File not found: {file_path}")
        else:
            print("fake_data directory not found")
        
        print("Spark test completed successfully!")
        spark.stop()
    except Exception as e:
        print(f"Error during Spark test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_spark_csv()
