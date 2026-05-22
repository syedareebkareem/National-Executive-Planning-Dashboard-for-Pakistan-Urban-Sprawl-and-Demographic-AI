import os
import shutil
from pathlib import Path
from pyspark.sql import SparkSession

try:
    # 1. Trace the shortcut to the real Java folder
    java_cmd = shutil.which("java")
    if not java_cmd:
        raise Exception("Java command not found.")
    
    real_java_exe = Path(java_cmd).resolve()
    java_home = str(real_java_exe.parent.parent)
    
    os.environ["JAVA_HOME"] = java_home
    print(f"[Info] Found real Java at: {java_home}")

    # 2. Set Hadoop path
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    
    # 3. Start Spark
    spark = SparkSession.builder.appName("PakistanProject").getOrCreate()
    print("\nSUCCESS! Spark Version:", spark.version)

except Exception as e:
    print("\nERROR:", e)
