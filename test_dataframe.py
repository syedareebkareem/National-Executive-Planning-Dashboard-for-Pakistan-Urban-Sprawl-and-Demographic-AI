import os
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Apply your Java 11 fix dynamically
project_root = os.getcwd()
java11_dir = os.path.join(project_root, "java11_folder")
for item in os.listdir(java11_dir):
    if "jdk" in item.lower():
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)
        break
os.environ['HADOOP_HOME'] = r'C:\hadoop'

try:
    spark = SparkSession.builder.appName("Task1_Test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- Running Big Data Engine Test ---")
    
    # Create fake data
    data = [Row(District="Lahore", Population=13000000),
            Row(District="Karachi", Population=16000000),
            Row(District="Islamabad", Population=1200000)]
    
    # Process through Spark DataFrame
    df = spark.createDataFrame(data)
    df.show()
    
    print("Test Passed: Spark is fully ready for Task 2!")
    spark.stop()

except Exception as e:
    print("\nTest Failed:", e)
