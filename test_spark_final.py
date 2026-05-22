import os
from pyspark.sql import SparkSession

# Force Python to use our downloaded Java 11
os.environ['JAVA_HOME'] = r'A:\BSIT-6A\pakistan-urban-sprawl\java11_folder\jdk-11.0.25+9'
os.environ['HADOOP_HOME'] = r'C:\hadoop'

try:
    spark = SparkSession.builder.appName("PakistanProject").getOrCreate()
    print("\n==================================")
    print("SUCCESS! Spark Version:", spark.version)
    print("==================================\n")
except Exception as e:
    print("\nERROR:", e)
