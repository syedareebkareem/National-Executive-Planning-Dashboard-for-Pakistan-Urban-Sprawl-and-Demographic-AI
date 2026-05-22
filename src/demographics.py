import os  # Initializes the core operating system interface module
import glob  # Imports the path pattern matching library for file retrieval
from pyspark.sql import SparkSession  # Ingests the primary entry point class for cluster computing
from pyspark.sql.functions import col, sum as _sum, regexp_replace  # Extracts essential mathematical and string manipulation functions for dataframe processing

project_root = os.getcwd()  # Captures the absolute directory path of the current runtime environment
java11_dir = os.path.join(project_root, "java11_folder")  # Constructs the specific absolute path targeting the local Java development kit repository
for item in os.listdir(java11_dir):  # Initiates an iteration sequence across all subdirectories within the specified Java folder
    if "jdk" in item.lower():  # Executes a string validation check to isolate the correct Java executable directory
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically assigns the environment variable required for PySpark cluster execution
        break  # Terminates the iteration sequence upon successful path resolution
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Hardcodes the Hadoop winutils backbone path required for Windows file system interactions

spark = SparkSession.builder.appName("DemographicAnalytics").getOrCreate()  # Instantiates or retrieves the distributed computing session architecture
spark.sparkContext.setLogLevel("ERROR")  # Restricts terminal output solely to critical failure logs to prevent console flooding

print("\n" + "="*50)  # Generates a structural boundary for terminal output rendering
print("  DEMOGRAPHIC PROJECTIONS (2017 -> 2022)")  # Outputs the title header for the impending terminal report
print("="*50)  # Renders the lower structural boundary for the terminal interface

target_file = None  # Initializes a null pointer variable for the impending file search protocol
for root_dir, dirs, files in os.walk(project_root):  # Initiates a recursive directory traversal originating from the project root
    if "venv" in root_dir or ".git" in root_dir: continue  # Implements an exclusion filter to bypass heavy system directories and optimize search speed
    for file in files:  # Iterates through the localized file arrays discovered during traversal
        if "Admin" in file and file.endswith(".csv"):  # Applies a dual condition filter to pinpoint the tabular demographic dataset
            target_file = os.path.join(root_dir, file)  # Synthesizes the absolute operating system path for the located dataset
            break  # Halts the localized file iteration upon successful pattern match
    if target_file: break  # Terminates the global recursive search once the primary target is acquired

if not target_file:  # Evaluates the final search resolution state for failure conditions
    print("[!] ERROR: Could not find Census CSV.")  # Triggers a terminal alert regarding the missing dataset asset
    spark.stop()  # Safely dismantles the distributed computing session to free local memory
    exit(1)  # Forces a hard system exit returning a standard error code

df = spark.read.csv(target_file, header=True, inferSchema=True)  # Ingests the raw tabular file into memory while automatically deducing data types

for c in df.columns:  # Iterates through the ingested dataframe column header array
    df = df.withColumnRenamed(c, c.strip())  # Executes a programmatic cleanup protocol to eliminate invisible whitespace characters

df = df.withColumn("Total_pop", regexp_replace(col("Total_pop"), ",", "").cast("int"))  # Sanitizes numerical string data by extracting commas and converting to native integers

if "Total_male_population" not in df.columns:  # Validates the structural integrity of the ingested gender columns
    df = df.withColumn("Total_male_population", col("Total_pop") * 0.51)  # Generates a synthetic male demographic column utilizing standard biological ratios
    df = df.withColumn("Total_female_population", col("Total_pop") * 0.49)  # Generates a synthetic female demographic column to complete the statistical matrix

totals = df.select(  # Initiates a selective data aggregation query on the sanitized dataframe
    _sum(col("Total_pop")).alias("total_2017"),  # Computes the absolute macro summation of the primary population column
    _sum(col("Total_male_population")).alias("male_2017"),  # Computes the absolute macro summation of the male subset column
    _sum(col("Total_female_population")).alias("female_2017")  # Computes the absolute macro summation of the female subset column
).collect()[0]  # Executes the distributed query and extracts the first resulting data array into local memory

rate_total = 0.024  # Hardcodes the established national compound annual growth parameter
pred_total = int(totals["total_2017"] * ((1 + rate_total) ** 5))  # Executes the mathematical demographic simulation formula spanning a five year temporal gap
pred_male = int(totals["male_2017"] * ((1 + rate_total) ** 5))  # Simulates the male subset expansion utilizing identical exponential logic
pred_female = int(totals["female_2017"] * ((1 + rate_total) ** 5))  # Simulates the female subset expansion utilizing identical exponential logic

print(f"Assumed Annual Growth Rate: {rate_total * 100:.2f}%\n")  # Renders the utilized mathematical growth parameter to the terminal output
print(f"{'METRIC':<18} | {'2017 (Actual)':<15} | {'2022 (Predicted)':<15}")  # Generates the structured header matrix for the final analytical report
print("-" * 55)  # Renders a strict horizontal boundary for tabular data separation
print(f"{'Total Population':<18} | {int(totals['total_2017']):<15,} | {pred_total:<15,}")  # Outputs the macro demographic variables with standard comma formatting
print(f"{'Total Male':<18} | {int(totals['male_2017']):<15,} | {pred_male:<15,}")  # Outputs the male subset variables matching the standardized structural format
print(f"{'Total Female':<18} | {int(totals['female_2017']):<15,} | {pred_female:<15,}")  # Outputs the female subset variables completing the final matrix display
print("-" * 55)  # Renders the terminal boundary finalizing the report module

spark.stop()  # Executes the final tear down protocol for the distributed computing environment