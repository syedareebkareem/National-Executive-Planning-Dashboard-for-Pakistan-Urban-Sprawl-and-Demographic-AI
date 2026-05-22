import os  # Establishes connection to core operating system directory navigation protocols
import glob  # Imports the path pattern matching library for dynamic file retrieval
import rasterio  # Ingests the specialized geospatial imaging library for satellite map extraction
import numpy as np  # Loads the advanced numerical processing engine for massive matrix calculations
import pandas as pd  # Integrates the secondary tabular data manipulation framework
from pyspark.sql import SparkSession  # Retrieves the primary cluster computing session architecture
from pyspark.sql.functions import col, lit, round  # Extracts columnar manipulation literal value assignment and rounding functions
project_root = os.getcwd()  # Captures the absolute path of the current Python execution environment
java11_dir = os.path.join(project_root, "java11_folder")  # Synthesizes the exact path targeting the local Java virtual machine
for item in os.listdir(java11_dir):  # Initiates a recursive loop traversing the Java directory structure
    if "jdk" in item.lower():  # Validates the presence of the critical development kit execution environment
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically configures the absolute path required for PySpark cluster execution
        break  # Terminates the iteration sequence upon successful dependency resolution
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Hardcodes the static path for Windows specific big data utilities
spark = SparkSession.builder.appName("DataCleaning").config("spark.driver.memory", "4g").getOrCreate()  # Instantiates the cluster session allocating four gigabytes of primary memory
spark.sparkContext.setLogLevel("ERROR")  # Suppresses non critical terminal outputs to maintain console clarity
print("\n" + "="*35)  # Generates a structural boundary for the terminal output interface
print("  TASK 3 SPATIAL DATA CLEANING")  # Outputs the primary header for the coordinate extraction sequence
print("="*35)  # Renders the lower structural boundary for visual separation
wp_path = glob.glob(os.path.join(project_root, "data", "raw", "worldpop", "*.tif"))  # Synthesizes the wildcard path for the spatial satellite raster
if not wp_path:  # Evaluates the resolution state of the spatial file search protocol
    raise FileNotFoundError("WorldPop TIF file not found in data raw worldpop folder")  # Triggers a critical system abort if the satellite raster is missing
print(f"[+] Loading Raster {os.path.basename(wp_path[0])}")  # Signals the commencement of the geospatial memory ingestion protocol
with rasterio.open(wp_path[0]) as src:  # Initializes a secure context manager to open and read the massive satellite raster
    data = src.read(1)  # Extracts the first band of demographic data from the satellite image matrix
    mask = data > 0.1  # Generates a mathematical filtration mask to isolate populated pixels and discard uninhabited terrain
    rows, cols = np.where(mask)  # Identifies the exact row and column array indices that satisfy the population filtration mask
    print("[+] Extracting spatial coordinates...")  # Outputs a status update regarding the geospatial transformation sequence
    xs, ys = rasterio.transform.xy(src.transform, rows, cols)  # Calculates the physical latitude and longitude coordinates utilizing the image affine transformation matrix
    pops = data[rows, cols]  # Extracts the exact numerical population values corresponding to the isolated array indices
print(f"[+] Extracted {len(pops)} populated grid cells")  # Renders the aggregate volume of identified populated pixels to the terminal
if len(pops) > 3000000:  # Evaluates the extracted pixel array against the localized hardware memory limits
    print("[!] Downsampling to 3 million points for local processing...")  # Triggers the memory protection protocol to prevent system crashes
    np.random.seed(42)  # Freezes the mathematical randomizer to ensure absolute determinism across executions
    sample_indices = np.random.choice(len(pops), 3000000, replace=False)  # Generates a randomized selection array to extract exactly three million coordinates without duplication
    xs = np.array(xs)[sample_indices]  # Applies the randomized selection array to truncate the longitudinal coordinate matrix
    ys = np.array(ys)[sample_indices]  # Applies the randomized selection array to truncate the latitudinal coordinate matrix
    pops = pops[sample_indices]  # Applies the randomized selection array to truncate the demographic value matrix
print("[+] Converting to Distributed Spark DataFrame...")  # Signals the commencement of the memory transition protocol
pdf = pd.DataFrame({"lon": xs, "lat": ys, "population": pops})  # Constructs an intermediate tabular structure utilizing the truncated numerical arrays
df = spark.createDataFrame(pdf)  # Ingests the intermediate tabular structure into the primary distributed memory cluster
print("[+] Applying PySpark Transformations...")  # Outputs a status update regarding the algorithmic filtration sequence
df_clean = df.filter((col("lat") >= 23.0) & (col("lat") <= 37.0) & (col("lon") >= 60.0) & (col("lon") <= 77.0)).dropna()  # Applies a strict bounding box filter to isolate coordinates physically located within Pakistan and removes null data
df_clean = df_clean.withColumn("lat", round(col("lat"), 5)).withColumn("lon", round(col("lon"), 5)).withColumn("population", col("population").cast("double")).withColumn("year", lit(2020))  # Truncates coordinate precision converts demographics to floating point formats and injects the baseline temporal year
print(f"[+] Final Cleaned Row Count {df_clean.count()}")  # Computes and displays the absolute macro summation of the finalized spatial dataset
df_clean.show(5)  # Displays the initial five rows of the clean dataset for visual validation
out_dir = os.path.join(project_root, "data", "processed", "cleaned.parquet")  # Synthesizes the destination directory path for the finalized columnar dataset
print(f"[+] Saving optimized Big Data Parquet file to {out_dir}")  # Outputs the final export destination to the terminal interface
df_clean.write.mode("overwrite").parquet(out_dir)  # Serializes the structured spatial matrix to the local disk overwriting prior iterations
print("\nTask 3 Completed Successfully")  # Signals the absolute termination of the spatial cleaning sequence
spark.stop()  # Dismantles the distributed computing cluster to restore local hardware resources