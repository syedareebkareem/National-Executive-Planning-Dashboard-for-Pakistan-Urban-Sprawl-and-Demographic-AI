import os  # Imports standard operating system directory interfaces-
import glob  # Ingests pattern matching libraries for dynamic file retrieval
from pyspark.sql import SparkSession  # Extracts the core cluster computing session architecture
project_root = os.getcwd()  # Captures the absolute directory path of the active runtime
java11_dir = os.path.join(project_root, "java11_folder")  # Constructs the targeted path for the Java virtual machine repository
for item in os.listdir(java11_dir):  # Initiates an iteration sequence traversing the local Java directory
    if "jdk" in item.lower():  # Validates the presence of the development kit execution environment
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically configures the critical environment variable for PySpark
        break  # Terminates the iteration sequence upon successful path resolution
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Establishes the static path for Windows specific big data execution utilities
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()  # Instantiates the primary memory cluster for data processing
spark.sparkContext.setLogLevel("ERROR")  # Suppresses non critical terminal outputs to maintain console clarity
print("\n" + "="*30)  # Generates a structural boundary for the terminal output interface
print("  TASK 2 DATA INGESTION")  # Outputs the primary header for the file loading sequence
print("="*30)  # Renders the lower structural boundary for visual separation
census_path = os.path.join(project_root, "data", "raw", "census", "*.csv")  # Synthesizes the wildcard path for tabular demographic data
census_files = glob.glob(census_path)  # Executes the file system search capturing matching structural datasets
if census_files:  # Evaluates the resolution state of the file search protocol
    print(f"[+] Loading Census {os.path.basename(census_files[0])}")  # Outputs the name of the acquired demographic dataset
    df_census = spark.read.csv(census_files[0], header=True, inferSchema=True)  # Ingests the tabular file into distributed memory while deducing types
    print(f"    Row Count {df_census.count()}")  # Computes and displays the absolute macro summation of loaded dataset rows
    df_census.printSchema()  # Renders the inferred data structure matrix to the terminal
    df_census.show(5)  # Displays the initial five rows of the ingested dataset for visual validation
else:  # Captures the failure state if the target demographic dataset is missing
    print("[!] ERROR No CSV found in data raw census folder")  # Triggers a terminal alert regarding the missing tabular asset
wp_path = os.path.join(project_root, "data", "raw", "worldpop", "*.tif")  # Synthesizes the wildcard path for the spatial satellite raster-
wp_files = glob.glob(wp_path)  # Executes the file system search capturing the target spatial map
if wp_files:  # Evaluates the resolution state of the spatial file search protocol
    print(f"[+] WorldPop File Detected {os.path.basename(wp_files[0])}")  # Outputs the successful detection of the high resolution coordinate raster
    print("    Ready for pixel to coordinate transformation in Task 3")  # Confirms readiness for impending mathematical spatial extraction
else:  # Captures the failure state if the target spatial raster is missing
    print("[!] ERROR No TIF found in data raw worldpop folder")  # Triggers a terminal alert detailing the missing spatial map asset
print("\nIngestion Script Finished Successfully")  # Signals the absolute termination of the data loading sequence
spark.stop()  # Dismantles the distributed computing cluster to free local hardware memory-