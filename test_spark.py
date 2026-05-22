import os
from pyspark.sql import SparkSession

try:
    # 1. Find the real Java folder automatically
    java_dir = r"C:\Program Files\Java"
    jdks = [os.path.join(java_dir, d) for d in os.listdir(java_dir) if "jdk" in d.lower() or "jre" in d.lower()]
    
    if jdks:
        os.environ["JAVA_HOME"] = jdks[-1]
        print(f"[Info] Forcing JAVA_HOME to: {jdks[-1]}")
    else:
        print("[Error] No JDK found in C:\\Program Files\\Java")
    
    # 2. Set Hadoop path
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    
    # 3. Start Spark
    spark = SparkSession.builder.appName("PakistanProject").getOrCreate()
    print("\nSUCCESS! Spark Version:", spark.version)

except Exception as e:
    print("\nERROR:", e)
