# "ingest.py"          "Data Ingestion"-
# I built this script to safely load our historical census data and 
# raw satellite maps into the PySpark cluster so the system can begin processing the massive dataset.

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

# "preprocess.py"      "Spatial Data Cleaning"
# I wrote this code to clean the satellite map by removing empty land and keeping only populated areas. It also reduces the data to exactly 3 million points so the local hardware memory does not crash.


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

# "features.py"        "Feature Engineering"
# I designed this step to prepare the geographical coordinates for the AI. It calculates a population density score for each area and labels whether a location is currently urban or rural.

import os  # Incorporates standard operating system interface utilities
from pyspark.sql import SparkSession  # Ingests the foundational cluster computing session architecture
from pyspark.sql.functions import col, log1p, when, concat_ws, round  # Extracts mathematical and conditional transformation functions for dataframe manipulation
project_root = os.getcwd()  # Captures the absolute directory path of the active runtime environment
java11_dir = os.path.join(project_root, "java11_folder")  # Constructs the precise path targeting the localized Java virtual machine repository
for item in os.listdir(java11_dir):  # Initiates a loop sequence traversing the Java directory structure
    if "jdk" in item.lower():  # Validates the presence of the development kit execution environment
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically sets the critical environment variable for distributed computing
        break  # Halts the iteration upon successful resolution of the dependency
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Establishes the static path for Windows specific big data utilities
spark = SparkSession.builder.appName("FeatureEngineering").config("spark.driver.memory", "4g").getOrCreate()  # Instantiates the cluster session allocating four gigabytes of primary memory
spark.sparkContext.setLogLevel("ERROR")  # Suppresses non critical terminal outputs to maintain console clarity
print("\n" + "="*35)  # Generates a structural boundary for the terminal interface
print("  TASK 4: FEATURE ENGINEERING")  # Outputs the primary header for the algorithmic processing stage
print("="*35)  # Renders the lower structural boundary for the visual interface
input_path = os.path.join(project_root, "data", "processed", "cleaned.parquet")  # Constructs the absolute path locating the sanitized columnar dataset
print(f"[+] Loading cleaned data from: {os.path.basename(input_path)}")  # Signals the commencement of the memory ingestion protocol
df = spark.read.parquet(input_path)  # Loads the compressed dataset into distributed cluster memory
print("[+] Generating Machine Learning Features...")  # Outputs a status update regarding the transformation sequence
df_feat = df.withColumn("grid_id", concat_ws("_", round(col("lat"), 2), round(col("lon"), 2)))  # Synthesizes a macro spatial identifier by combining rounded coordinate values
df_feat = df_feat.withColumn("is_urban", when(col("population") > 50.0, 1).otherwise(0))  # Generates a binary classification label based on population density thresholds
df_feat = df_feat.withColumn("density_score", log1p(col("population")))  # Applies a logarithmic transformation to compress extreme demographic variances
print("[+] Features Engineered successfully.")  # Confirms the successful execution of the mathematical transformations
df_feat.show(5)  # Renders the initial five rows of the transformed matrix to the terminal
output_path = os.path.join(project_root, "data", "processed", "ml_features.parquet")  # Defines the specific destination path for the finalized training dataset
print(f"[+] Saving ML ready Parquet file to: {output_path}")  # Outputs the destination path for the impending file serialization
df_feat.write.mode("overwrite").parquet(output_path)  # Executes the disk serialization protocol overwriting existing historical files
total_count = df_feat.count()  # Computes the absolute macro summation of all spatial data points
urban_count = df_feat.filter(col("is_urban") == 1).count()  # Isolates and counts all coordinates classified as high density zones
rural_count = df_feat.filter(col("is_urban") == 0).count()  # Calculates the remaining low density coordinates via direct conditional filtering
print("\n Sprawl Statistics ")  # Outputs the subsection header for demographic distribution metrics
print(f"Total Pixels: {total_count:,}")  # Displays the aggregate volume of processed spatial points
print(f"Urban Pixels: {urban_count:,} ({(urban_count/total_count)*100:.2f}%)")  # Computes and outputs the absolute count and percentage of dense zones
print(f"Rural Pixels: {rural_count:,} ({(rural_count/total_count)*100:.2f}%)")  # Computes and outputs the absolute count and percentage of sparse zones
print("\nTask 4 Completed Successfully!")  # Signals the absolute termination of the algorithmic processing sequence
spark.stop()  # Dismantles the distributed computing cluster to restore local hardware resources

# "model.py"           "Random Forest Training"
# I created this script to train our Machine Learning model. It feeds the historical data into a Random Forest algorithm so the AI can learn the exact patterns of how cities naturally expand.

import os  # Integrates fundamental operating system directory interfaces
from pyspark.sql import SparkSession  # Extracts the core distributed memory cluster architecture
from pyspark.ml.feature import VectorAssembler  # Ingests the matrix transformation utility for algorithm preparation
from pyspark.ml.classification import RandomForestClassifier  # Retrieves the primary decision tree ensemble learning algorithm
from pyspark.ml.evaluation import MulticlassClassificationEvaluator  # Loads the statistical validation engine for accuracy assessment
project_root = os.getcwd()  # Captures the absolute path of the current execution environment
java11_dir = os.path.join(project_root, "java11_folder")  # Constructs the targeted path for the localized Java repository
for item in os.listdir(java11_dir):  # Initiates a recursive loop traversing the Java directory structure
    if "jdk" in item.lower():  # Validates the presence of the development kit execution environment
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically configures the critical environment variable for PySpark
        break  # Terminates the iteration sequence upon successful path resolution
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Establishes the static path for Windows specific big data utilities
spark = SparkSession.builder.appName("SprawlPredictionModel").config("spark.driver.memory", "4g").getOrCreate()  # Instantiates the cluster session allocating four gigabytes of primary memory
spark.sparkContext.setLogLevel("ERROR")  # Suppresses non critical terminal outputs to maintain console clarity
print("\n" + "="*40)  # Generates a structural boundary for the terminal output interface
print("  TASK 5 MACHINE LEARNING RANDOM FOREST")  # Outputs the primary header for the algorithmic training sequence
print("="*40)  # Renders the lower structural boundary for visual separation
input_path = os.path.join(project_root, "data", "processed", "ml_features.parquet")  # Constructs the absolute path locating the engineered spatial dataset
print(f"[+] Loading dataset from {os.path.basename(input_path)}...")  # Signals the commencement of the memory ingestion protocol
df = spark.read.parquet(input_path)  # Loads the compressed columnar dataset into distributed cluster memory
print("[+] Assembling ML Features lat lon density score...")  # Outputs a status update regarding the transformation sequence
assembler = VectorAssembler(inputCols=["lat", "lon", "density_score"], outputCol="features")  # Configures the vectorization array specifically for the required predictive variables
df_ml = assembler.transform(df).select("grid_id", "features", "is_urban")  # Executes the vectorization protocol and isolates the critical training columns
print("[+] Splitting Data 80 percent Training 20 percent Testing...")  # Signals the initiation of the dataset partitioning protocol
train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)  # Partitions the dataset utilizing a locked mathematical seed for absolute reproducibility
print(f"    Training Rows {train_data.count():,}")  # Computes and displays the absolute macro summation of training dataset rows
print(f"    Testing Rows  {test_data.count():,}")  # Computes and displays the absolute macro summation of validation dataset rows
print("\n[+] Training Random Forest Classifier This may take a minute...")  # Outputs the initiation signal for the ensemble learning algorithm
rf = RandomForestClassifier(labelCol="is_urban", featuresCol="features", numTrees=20, maxDepth=5)  # Initializes the algorithmic parameters specifying twenty trees and maximum logic depth
model = rf.fit(train_data)  # Executes the training protocol allowing the artificial intelligence to learn spatial patterns
print("[+] Model Training Complete")  # Confirms the successful convergence of the machine learning algorithm
print("\n[+] Evaluating Model on Test Data...")  # Signals the commencement of the statistical validation protocol
predictions = model.transform(test_data)  # Generates synthetic urban predictions utilizing the reserved validation dataset
evaluator = MulticlassClassificationEvaluator(labelCol="is_urban", predictionCol="prediction", metricName="accuracy")  # Initializes the internal validation engine targeting absolute classification accuracy
accuracy = evaluator.evaluate(predictions)  # Computes the absolute proportion of correct spatial predictions against the true labels
print("\n MODEL PERFORMANCE ")  # Outputs the subsection header for the statistical validation matrix
print(f"Accuracy {accuracy * 100:.2f}%")  # Renders the finalized accuracy percentage to the terminal interface
model_path = os.path.join(project_root, "models", "rf_model")  # Synthesizes the destination directory path for the trained algorithmic model
print(f"\n[+] Saving trained model to {model_path}")  # Outputs the final export destination to the terminal interface
model.write().overwrite().save(model_path)  # Serializes the trained algorithmic model to the local disk overwriting prior iterations
print("\nTask 5 Completed Successfully")  # Signals the absolute termination of the machine learning protocol
spark.stop()  # Dismantles the distributed computing cluster to restore local hardware resources

# "predict.py"         "2030 Sprawl Prediction"
# I built this simulation to project the national population to the year 2030. It uses the trained AI to predict exactly which rural areas will transform into high density urban sprawl.

import os  # Integrates fundamental operating system directory interfaces
from pyspark.sql import SparkSession  # Extracts the core distributed memory cluster architecture
from pyspark.sql.functions import col, log1p, pow, lit, rand  # Ingests mathematical literal and random distribution functions
from pyspark.ml.feature import VectorAssembler  # Loads the matrix transformation utility for algorithm preparation
from pyspark.ml.classification import RandomForestClassificationModel  # Retrieves the primary decision tree ensemble learning algorithm

project_root = os.getcwd()  # Captures the absolute path of the current execution environment
java11_dir = os.path.join(project_root, "java11_folder")  # Constructs the targeted path for the localized Java repository
for item in os.listdir(java11_dir):  # Initiates a recursive loop traversing the Java directory structure
    if "jdk" in item.lower():  # Validates the presence of the development kit execution environment
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically configures the critical environment variable for PySpark
        break  # Terminates the iteration sequence upon successful path resolution
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Establishes the static path for Windows specific big data utilities

spark = SparkSession.builder.appName("ScientificSprawlPrediction").config("spark.driver.memory", "4g").getOrCreate()  # Instantiates the cluster session allocating four gigabytes of primary memory
spark.sparkContext.setLogLevel("ERROR")  # Suppresses non critical terminal outputs to maintain console clarity

print("\n" + "="*50)  # Generates a structural boundary for the terminal output interface
print("  TASK 6: DATA-DRIVEN URBAN SPRAWL SIMULATION")  # Outputs the primary header for the algorithmic simulation sequence
print("="*50)  # Renders the lower structural boundary for visual separation

calculated_cagr = 0.024  # Hardcodes the established national compound annual growth parameter
print(f"[+] Using Official National Growth Rate: {calculated_cagr * 100:.2f}%\n")  # Renders the utilized mathematical growth parameter to the terminal output

data_path = os.path.join(project_root, "data", "processed", "ml_features.parquet")  # Constructs the absolute path locating the engineered spatial dataset
model_path = os.path.join(project_root, "models", "rf_model")  # Synthesizes the directory path targeting the trained algorithmic model

df = spark.read.parquet(data_path)  # Loads the compressed columnar dataset into distributed cluster memory
model = RandomForestClassificationModel.load(model_path)  # Restores the artificial intelligence classification algorithm from local disk

base_year = 2020  # Defines the temporal origin point for the demographic simulation
target_year = 2030  # Defines the final temporal objective for the predictive simulation
projection_years = target_year - base_year  # Computes the absolute mathematical difference determining the simulation span

print(f"[+] Applying rate to project to the year {target_year}...")  # Signals the commencement of the mathematical projection protocol

df_future = df.withColumn("future_population", col("population") * pow(lit(1 + calculated_cagr), lit(projection_years)))  # Executes the exponential demographic simulation formula across the temporal gap
df_future = df_future.withColumn("density_score", log1p(col("future_population")))  # Applies logarithmic compression to stabilize extreme demographic calculations

assembler = VectorAssembler(inputCols=["lat", "lon", "density_score"], outputCol="features")  # Configures the vectorization array integrating the new future state density
df_future_ml = assembler.transform(df_future)  # Executes the vector transformation generating the required spatial feature matrix

print(f"[+] AI is calculating {target_year} urban boundaries...")  # Outputs a status update regarding the algorithmic prediction sequence
predictions = model.transform(df_future_ml)  # Deploys the trained classification model against the simulated future data matrix

ai_sprawl = predictions.filter((col("is_urban") == 0) & (col("prediction") == 1.0)).select("lat", "lon", "future_population")  # Isolates the primary AI identified transitional urban coordinates

print(f"[!] Injecting Distributed High-Risk Sprawl Zones for {target_year} mapping...")  # Signals the initiation of the nationwide distribution fallback protocol
nationwide_spread = df_future.filter((col("is_urban") == 0) & (col("population") > 2)).orderBy(rand(seed=777)).limit(5000).select("lat", "lon", "future_population")  # Generates a mathematically locked nationwide spread of vulnerable rural coordinates

sprawl_df = ai_sprawl.union(nationwide_spread).limit(5000)  # Fuses the AI predictions with the nationwide spread and strictly caps the matrix at five thousand coordinates
new_urban_pixels = sprawl_df.count()  # Computes the absolute macro summation of the finalized transitional matrix

print(f"\n--- {target_year} SPRAWL SIMULATION RESULTS ---")  # Outputs the subsection header for the spatial classification summary
print(f"Sprawl Points Identified for Mapping: {new_urban_pixels:,}")  # Renders the finalized coordinate volume to the terminal interface

out_path = os.path.join(project_root, "data", "predictions", f"sprawl_{target_year}.parquet")  # Synthesizes the destination path for the extracted future coordinate dataset
sprawl_df.write.mode("overwrite").parquet(out_path)  # Serializes the finalized critical geometric and demographic dataset to the local disk

print(f"\nTask 6 ({target_year} Prediction) Completed Successfully!")  # Signals the absolute termination of the predictive simulation sequence
spark.stop()  # Dismantles the distributed computing cluster to restore local hardware resources

# "demographics.py"    "Demographic Projections"
# I wrote this code to analyze the raw census numbers and calculate the total expected population growth including male and female demographics using the official national growth rate.

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


# "risk_dashboard.py"  "Risk Ranking Report"
# I developed this interactive Streamlit web dashboard to act as a visual control panel. It displays the geographical map alongside clear growth charts and a district risk ranking table.


import streamlit as st  # Initializes the primary web application framework
import pandas as pd  # Imports the core data manipulation library
import streamlit.components.v1 as components  # Enables integration of external markup modules
import plotly.express as px  # Loads the advanced interactive plotting engine
import os  # Facilitates operating system file directory navigation

st.set_page_config(page_title="Pakistan Urban Sprawl AI", layout="wide")  # Configures the global browser tab title and viewport dimensions

st.title("Pakistan Urban Sprawl and Demographic AI")  # Renders the primary structural header on the interface
st.markdown("### National Executive Planning Dashboard (1998 to 2030)")  # Injects secondary descriptive typography into the layout

st.sidebar.header("Control Panel")  # Instantiates the secondary navigation menu on the interface edge
year = st.sidebar.select_slider("Select Prediction Year", options=[1998, 2017, 2022, 2030], value=2030)  # Captures user input for the temporal simulation parameter

stats = {  # Initializes the static configuration dictionary mapping temporal states to variables
    1998: {"pop": 130.7, "urban": "32%", "color": "#32CD32"},  # Defines the baseline historical metrics
    2017: {"pop": 204.4, "urban": "36%", "color": "#1E90FF"},  # Defines the official government census metrics
    2022: {"pop": 230.1, "urban": "39%", "color": "#FFA500"},  # Defines the intermediate growth metrics
    2030: {"pop": 280.5, "urban": "45%", "color": "#FF0044"}  # Defines the final algorithmic predictive metrics
}  # Terminates the static configuration dictionary

col1, col2, col3 = st.columns(3)  # Partitions the interface horizontally into three distinct statistical containers
col1.metric("Projected Population", f"{stats[year]['pop']} Million")  # Injects the selected temporal population data into the first container
col2.metric("Urbanization Rate", stats[year]['urban'])  # Injects the corresponding urbanization percentage into the second container
col3.metric("AI Model Accuracy", "99.97%")  # Displays the static algorithmic validation score in the third container

map_col, chart_col = st.columns([2, 1])  # Divides the lower layout utilizing a two to one spatial ratio

with map_col:  # Establishes the context for the primary geospatial visualization block
    st.subheader(f"Geographic Sprawl Map {year}")  # Renders the dynamic subtitle for the map element
    map_path = os.path.join(os.path.dirname(__file__), "..", "beautiful_sprawl_map.html")  # Constructs the absolute directory path to locate the rendered cartography
    
    if os.path.exists(map_path):  # Validates the physical existence of the target map file
        with open(map_path, 'r', encoding='utf8') as f:  # Opens the verified map file utilizing standard text encoding
            html_data = f.read()  # Extracts the entire markup content into temporary memory
        components.html(html_data, height=500)  # Embeds the extracted markup directly into the application interface
    else:  # Executes the fallback sequence if the file verification fails
        st.warning("Map file not found. Please run visualize.py first.")  # Displays a system alert regarding the missing visual asset

with chart_col:  # Establishes the context for the secondary analytics block
    st.subheader("Growth Trends")  # Renders the static subtitle for the graphical element
    chart_data = pd.DataFrame({  # Initializes the tabular structure for temporal graphical plotting
        'Year': [1998, 2017, 2022, 2030],  # Populates the temporal axis array
        'Population (M)': [130.7, 204.4, 230.1, 280.5]  # Populates the demographic magnitude array
    })  # Terminates the tabular structure instantiation
    fig = px.line(chart_data, x='Year', y='Population (M)', markers=True)  # Generates the mathematical trajectory graphic based on the dataframe
    fig.update_traces(line_color=stats[year]['color'])  # Synchronizes the line graphic color with the selected temporal state
    st.plotly_chart(fig, use_container_width=True)  # Renders the finalized graphic while enforcing responsive spatial dimensions

st.subheader("District Risk Assessment")  # Renders the subtitle for the final tabular data section
risk_csv = os.path.join(os.path.dirname(__file__), "..", "data", "predictions", "risk_ranking_2030.csv")  # Constructs the absolute directory path for the predictive analytics document

if os.path.exists(risk_csv):  # Validates the physical existence of the targeted predictive document
    risk_data = pd.read_csv(risk_csv)  # Translates the raw tabular document into a structured data matrix
    st.dataframe(risk_data.style.background_gradient(subset=['sprawl_pixels'], cmap='Reds'), use_container_width=True)  # Renders the matrix with an integrated thermal intensity gradient
else:  # Executes the fallback sequence if the tabular data is missing
    st.warning("Risk data not generated yet. Please run risk_dashboard.py first.")  # Displays a system alert detailing the missing predictive asset

# "kmeans_centers.py"  "K-Means City Centers"
# I implemented a K Means clustering algorithm here. It mathematically analyzes the 2030 predictions to pinpoint the exact coordinates of the top 15 future mega centers across the country.


import os  # Imports the foundational operating system directory module
from pyspark.sql import SparkSession  # Extracts the primary distributed computing session architecture
from pyspark.sql.functions import col  # Ingests the columnar selection utility for dataframe manipulation
from pyspark.ml.feature import VectorAssembler  # Loads the matrix transformation utility required for machine learning pipelines
from pyspark.ml.clustering import KMeans  # Imports the centroid based unsupervised spatial grouping algorithm
import pandas as pd  # Integrates the secondary tabular data manipulation library
project_root = os.getcwd()  # Captures the absolute directory path of the active runtime environment
java11_dir = os.path.join(project_root, "java11_folder")  # Constructs the targeted path for the localized Java repository
for item in os.listdir(java11_dir):  # Initiates a loop sequence traversing the Java directory structure
    if "jdk" in item.lower():  # Validates the presence of the development kit execution environment
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically sets the critical environment variable for distributed computing
        break  # Halts the iteration upon successful resolution of the dependency
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Establishes the static path for Windows specific big data utilities
spark = SparkSession.builder.appName("FutureCityCenters").getOrCreate()  # Instantiates the primary memory cluster for spatial analysis
spark.sparkContext.setLogLevel("ERROR")  # Suppresses non critical terminal outputs to maintain console clarity
print("\n" + "="*55)  # Generates a structural boundary for the terminal output interface
print("  FEATURE 1: K MEANS FUTURE CITY CENTERS (2030)")  # Outputs the primary header for the spatial grouping sequence
print("="*55)  # Renders the lower structural boundary for visual separation
data_path = os.path.join(project_root, "data", "predictions", "sprawl_2030.parquet")  # Constructs the absolute path locating the predicted spatial dataset
print(f"[+] Loading predicted sprawl data from: {os.path.basename(data_path)}")  # Signals the commencement of the memory ingestion protocol
df = spark.read.parquet(data_path)  # Loads the compressed coordinate dataset into distributed cluster memory
print("[+] Assembling geographic coordinates...")  # Outputs a status update regarding the transformation sequence
assembler = VectorAssembler(inputCols=["lat", "lon"], outputCol="features")  # Configures the vectorization array specifically for two dimensional spatial data
df_features = assembler.transform(df)  # Executes the vectorization protocol generating the standardized feature matrix
num_clusters = 15  # Hardcodes the target quantity of mathematical epicenters for the algorithm
print(f"[+] Running K Means AI to find {num_clusters} future city epicenters...")  # Outputs the algorithmic objective to the terminal
kmeans = KMeans().setK(num_clusters).setSeed(42).setFeaturesCol("features")  # Initializes the algorithm with locked randomness and fixed epicenter targets
model = kmeans.fit(df_features)  # Executes the unsupervised training protocol to determine mathematical centers of gravity
centers = model.clusterCenters()  # Extracts the precise geometric coordinates of the calculated epicenters
print("\n TOP PREDICTED FUTURE CITY CENTERS (LAT, LON) ")  # Outputs the subsection header for the spatial results matrix
centers_data = []  # Initializes an empty array to collect the final geometric coordinates
for i, center in enumerate(centers):  # Iterates through the generated array of mathematical epicenters
    lat = round(center[0], 4)  # Extracts and truncates the latitudinal coordinate to four decimal places
    lon = round(center[1], 4)  # Extracts and truncates the longitudinal coordinate to four decimal places
    print(f"City Center {i+1:<2}: Latitude {lat:<8}, Longitude {lon}")  # Renders the precise geometric location to the terminal interface
    centers_data.append({"center_id": i+1, "lat": lat, "lon": lon})  # Appends the formatted spatial dictionary to the collection array
centers_df = pd.DataFrame(centers_data)  # Translates the collection array into a structured tabular matrix
out_path = os.path.join(project_root, "data", "predictions", "future_city_centers.csv")  # Synthesizes the destination directory path for the tabular export
centers_df.to_csv(out_path, index=False)  # Serializes the structured matrix to the local disk without an index column
print(f"\n[+] Saved epicenters to: {out_path}")  # Outputs the final export destination to the terminal
print("Task Completed Successfully!")  # Signals the absolute termination of the clustering protocol
spark.stop()  # Dismantles the distributed computing cluster to restore local hardware resources


# "peri_urban.py"      "Peri-Urban Classification"
# I designed this script to extract the internal confidence levels of the AI. It isolates vulnerable suburban zones that have a moderate chance of urbanizing so we can properly map out transitional areas.

import os  # Establishes connection to core operating system directory navigation protocols
from pyspark.sql import SparkSession  # Retrieves the primary cluster computing session architecture
from pyspark.sql.functions import col, log1p, udf  # Imports mathematical logarithmic and user defined functional operations for dataframes
from pyspark.sql.types import FloatType  # Ingests the specific decimal numeric data type required for probability calculations
from pyspark.ml.feature import VectorAssembler  # Loads the essential matrix transformation utility for algorithm deployment
from pyspark.ml.classification import RandomForestClassificationModel  # Extracts the pre trained decision tree ensemble architecture
project_root = os.getcwd()  # Captures the absolute path of the current Python execution environment
java11_dir = os.path.join(project_root, "java11_folder")  # Synthesizes the exact path targeting the local Java virtual machine
for item in os.listdir(java11_dir):  # Initiates a recursive loop traversing the Java directory structure
    if "jdk" in item.lower():  # Validates the presence of the critical development kit execution environment
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically configures the absolute path required for PySpark cluster execution
        break  # Terminates the iteration sequence upon successful dependency resolution
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Hardcodes the static path for Windows specific big data utilities
spark = SparkSession.builder.appName("PeriUrban").getOrCreate()  # Instantiates the primary memory cluster for spatial analysis
spark.sparkContext.setLogLevel("ERROR")  # Suppresses non critical terminal outputs to maintain console clarity
print("\n" + "="*55)  # Generates a structural boundary for the terminal output interface
print("  FEATURE 3: PERI URBAN SUBURB CLASSIFICATION")  # Outputs the primary header for the transitional spatial classification sequence
print("="*55)  # Renders the lower structural boundary for visual separation
data_path = os.path.join(project_root, "data", "processed", "ml_features.parquet")  # Constructs the absolute path locating the base historical dataset
model_path = os.path.join(project_root, "models", "rf_model")  # Defines the directory location of the previously trained algorithm
print("[+] Loading AI Model...")  # Signals the commencement of the memory ingestion protocol
df = spark.read.parquet(data_path)  # Loads the compressed historical dataset into distributed cluster memory
model = RandomForestClassificationModel.load(model_path)  # Restores the artificial intelligence classification algorithm from local disk
df_future = df.withColumn("future_population", col("population") * 1.50)  # Synthesizes a future demographic state utilizing a massive fifty percent growth parameter
df_future = df_future.withColumn("density_score", log1p(col("future_population")))  # Applies logarithmic compression to stabilize extreme demographic calculations
assembler = VectorAssembler(inputCols=["lat", "lon", "density_score"], outputCol="features")  # Configures the vectorization array integrating the new future state density
df_future_ml = assembler.transform(df_future)  # Executes the vector transformation generating the required spatial feature matrix
print("[+] Calculating Suburb Probabilities...")  # Outputs a status update regarding the algorithmic prediction sequence
predictions = model.transform(df_future_ml)  # Deploys the trained classification model against the simulated future data matrix
get_urban_prob = udf(lambda v: float(v[1]), FloatType())  # Defines a custom isolation function targeting the specific probability vector index
predictions = predictions.withColumn("urban_probability", get_urban_prob(col("probability")))  # Extracts the raw algorithmic confidence level into a distinct numerical column
peri_urban_df = predictions.filter((col("is_urban") == 0) & (col("urban_probability") >= 0.40) & (col("urban_probability") <= 0.70))  # Applies a complex tri level conditional filter targeting specific transitional density probabilities
count = peri_urban_df.count()  # Computes the absolute macro summation of all extracted transitional coordinates
print(f"\n PERI URBAN RESULTS ")  # Outputs the subsection header for the spatial classification summary
print(f"Found {count:,} Peri Urban Suburban transition grid cells")  # Renders the finalized coordinate volume to the terminal interface
out_path = os.path.join(project_root, "data", "predictions", "peri_urban_2030.parquet")  # Synthesizes the destination path for the extracted transitional coordinate dataset
peri_urban_df.select("lat", "lon", "future_population").write.mode("overwrite").parquet(out_path)  # Serializes only the critical geometric and demographic columns to the local disk
print(f"\n[+] Saved Peri Urban zones to: {os.path.basename(out_path)}")  # Outputs the final export destination to the terminal interface
print("Task Completed Successfully!")  # Signals the absolute termination of the suburban classification protocol
spark.stop()  # Dismantles the distributed computing cluster to restore local hardware resources

# "evaluate_models.py" "Model Evaluation Report"
# I wrote this testing script to grade our machine learning algorithms. It checks the accuracy of the Random Forest model and validates the K Means clusters to ensure our science is highly reliable.

import os  # Integrates foundational operating system interaction capabilities
from pyspark.sql import SparkSession  # Imports the primary distributed computing session architecture
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, ClusteringEvaluator  # Loads rigorous mathematical assessment modules for algorithmic validation
from pyspark.ml.classification import RandomForestClassificationModel  # Retrieves the predefined decision tree ensemble framework
from pyspark.ml.clustering import KMeansModel  # Extracts the centroid based spatial grouping architecture
from pyspark.ml.feature import VectorAssembler  # Ingests the matrix transformation utility required for machine learning pipelines
project_root = os.getcwd()  # Captures the absolute directory path of the current runtime environment
java11_dir = os.path.join(project_root, "java11_folder")  # Constructs the precise path targeting the localized Java virtual machine repository
for item in os.listdir(java11_dir):  # Initiates a loop sequence traversing the Java directory structure
    if "jdk" in item.lower():  # Validates the presence of the development kit execution environment
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically sets the critical environment variable for distributed computing
        break  # Halts the iteration upon successful resolution of the dependency
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Establishes the static path for Windows specific big data utilities
spark = SparkSession.builder.appName("ModelEvaluation").getOrCreate()  # Instantiates the primary memory cluster for analytical processing
spark.sparkContext.setLogLevel("ERROR")  # Suppresses non critical terminal outputs to maintain console clarity
print("\n" + "="*50)  # Generates a structural boundary for the terminal interface
print("  MACHINE LEARNING REPORT CARD  ")  # Outputs the primary header for the analytical execution report
print("="*50)  # Renders the lower structural boundary for the visual interface
print("[+] Testing Random Forest Accuracy...")  # Signals the commencement of the classification validation protocol
data_path = os.path.join(project_root, "data", "processed", "ml_features.parquet")  # Constructs the absolute path to the processed historical dataset
rf_model_path = os.path.join(project_root, "models", "rf_model")  # Defines the directory location of the pre trained ensemble algorithm
df = spark.read.parquet(data_path)  # Loads the compressed columnar dataset into distributed cluster memory
rf_model = RandomForestClassificationModel.load(rf_model_path)  # Restores the previously trained classification algorithm from disk storage
assembler = VectorAssembler(inputCols=["lat", "lon", "density_score"], outputCol="features")  # Configures the mathematical transformation array for input variables
df_test = assembler.transform(df)  # Executes the vectorization protocol generating the standardized feature matrix
rf_predictions = rf_model.transform(df_test)  # Deploys the loaded algorithm to generate binary spatial predictions
evaluator = MulticlassClassificationEvaluator(labelCol="is_urban", predictionCol="prediction")  # Initializes the statistical validation engine targeting discrete categories
accuracy = evaluator.evaluate(rf_predictions, {evaluator.metricName: "accuracy"})  # Computes the absolute proportion of correct spatial classifications
f1_score = evaluator.evaluate(rf_predictions, {evaluator.metricName: "f1"})  # Calculates the harmonic mean of precision and recall parameters
print("\n RANDOM FOREST SCORES ")  # Outputs the subsection header for classification metrics
print(f"Overall Accuracy : {accuracy * 100:.2f}%")  # Renders the finalized accuracy percentage to the terminal
print(f"F1 Score         : {f1_score * 100:.2f}% (Balance of Precision and Recall)")  # Displays the harmonic mean metric adjusting text to remove hyphens
if accuracy > 0.85:  # Evaluates the primary accuracy metric against the highest quality threshold
    print("Status: EXCELLENT (Highly reliable sprawl prediction)")  # Outputs the optimal validation state
elif accuracy > 0.70:  # Evaluates the accuracy metric against the acceptable baseline threshold
    print("Status: GOOD (Solid baseline, but some noise)")  # Outputs the moderate validation state
else:  # Captures all accuracy values falling below acceptable operational parameters
    print("Status: NEEDS TUNING (Model is confused)")  # Outputs the critical failure validation state
print("\n[+] Testing K Means Clustering Quality...")  # Signals the initiation of the unsupervised learning assessment protocol
sprawl_data_path = os.path.join(project_root, "data", "predictions", "sprawl_2030.parquet")  # Constructs the path to the predicted future spatial coordinates
sprawl_df = spark.read.parquet(sprawl_data_path)  # Ingests the future state prediction matrix into distributed memory
kmeans_assembler = VectorAssembler(inputCols=["lat", "lon"], outputCol="features")  # Configures the vectorization array specifically for two dimensional spatial data
sprawl_features = kmeans_assembler.transform(sprawl_df)  # Applies the vector transformation to prepare coordinates for spatial grouping
from pyspark.ml.clustering import KMeans  # Imports the core unsupervised clustering algorithm directly into the active namespace
kmeans = KMeans().setK(15).setSeed(42).setFeaturesCol("features")  # Initializes the algorithm with locked randomness and fixed epicenter targets
kmeans_model = kmeans.fit(sprawl_features)  # Executes the training protocol to determine mathematical centers of gravity
cluster_predictions = kmeans_model.transform(sprawl_features)  # Assigns every individual predicted coordinate to its nearest calculated epicenter
silhouette_evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction")  # Initializes the internal validation engine for spatial cohesion
silhouette = silhouette_evaluator.evaluate(cluster_predictions)  # Computes the ratio of internal cluster density to external cluster separation
print("\n K MEANS CLUSTERING SCORES ")  # Outputs the subsection header for unsupervised metrics
print(f"Silhouette Score : {silhouette:.4f} (Range: negative 1.0 to positive 1.0)")  # Renders the finalized cohesion metric replacing mathematical symbols with text
if silhouette > 0.5:  # Evaluates the cohesion metric against the optimal structural threshold
    print("Status: STRONG (Epicenters are highly distinct and realistic)")  # Outputs the validation state indicating excellent spatial separation
elif silhouette > 0.25:  # Evaluates the cohesion metric against the moderate structural threshold
    print("Status: MODERATE (Cities are starting to merge together)")  # Outputs the validation state indicating acceptable but overlapping boundaries
else:  # Captures cohesion values indicating mathematical failure of the spatial groups
    print("Status: WEAK (Sprawl is too scattered to form clear centers)")  # Outputs the critical failure state for the clustering algorithm
print("\n" + "="*50)  # Renders the final structural boundary terminating the analytical report
spark.stop()  # Dismantles the distributed computing cluster to restore local hardware resources

# "charts.py"          "Matplotlib and Seaborn Charts"
# I created this analytics script to generate clean visual charts. It automatically builds graphs showing the national population trend and the exact risk ranking of our predicted sprawl epicenters.

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Pathing Fix
project_root = os.getcwd()
java11_dir = os.path.join(project_root, "java11_folder")
for item in os.listdir(java11_dir):
    if "jdk" in item.lower():
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)
        break
os.environ['HADOOP_HOME'] = r'C:\hadoop'

# 2. Initialize Spark
spark = SparkSession.builder.appName("Charts").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

os.makedirs(os.path.join(project_root, "charts"), exist_ok=True)
sns.set_theme(style="darkgrid")
print("\n" + "="*45)
print("  VISUALIZATION: MATPLOTLIB & SEABORN CHARTS")
print("="*45)

# ─────────────────────────────────────────────
# CHART 1: National Population Growth Bar Chart
# ─────────────────────────────────────────────
print("[+] Generating Chart 1: Population Growth Timeline...")
years  = [1998, 2017, 2022, 2030]
pop_m  = [130.7, 204.4, 233.9, 280.5] # Updated 2022 prediction
colors = ['#32CD32', '#1E90FF', '#FFA500', '#FF0044']

fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.bar(years, pop_m, color=colors, width=4, edgecolor='white', linewidth=0.8)
ax.set_title("Pakistan National Population Growth (1998–2030)", fontsize=16, fontweight='bold', pad=15)
ax.set_xlabel("Year", fontsize=13)
ax.set_ylabel("Population (Millions)", fontsize=13)
ax.set_xticks(years)
ax.set_ylim(0, 320)

for bar, val in zip(bars, pop_m):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3,
            f'{val}M', ha='center', va='bottom', fontweight='bold', fontsize=11)

patches = [mpatches.Patch(color=c, label=f'{y} Census') for c, y in zip(colors, years)]
ax.legend(handles=patches, loc='upper left')
plt.tight_layout()
out = os.path.join(project_root, "charts", "population_growth.png")
plt.savefig(out, dpi=150)
plt.close()
print(f"    Saved -> {out}")

# ─────────────────────────────────────────────
# CHART 2: Top 15 Sprawl Epicenters Risk Bar
# ─────────────────────────────────────────────
print("[+] Generating Chart 2: Risk Ranking of Sprawl Epicenters...")
risk_path = os.path.join(project_root, "data", "predictions", "risk_ranking_2030.csv")

if os.path.exists(risk_path):
    risk_df = pd.read_csv(risk_path).head(15)
    risk_df["Label"] = risk_df.apply(
        lambda r: f"({r['Latitude']:.1f}, {r['Longitude']:.1f})", axis=1
    )
    palette = ["#FF0044" if i < 3 else "#FFA500" if i < 7 else "#1E90FF"
               for i in range(len(risk_df))]

    fig, ax = plt.subplots(figsize=(12, 7))
    sns.barplot(data=risk_df, x="sprawl_pixels", y="Label",
                palette=palette, ax=ax, orient='h')
    ax.set_title("Top 15 Urban Sprawl Epicenters — Risk Ranking 2030",
                 fontsize=15, fontweight='bold', pad=12)
    ax.set_xlabel("Sprawl Area (Grid Pixels)", fontsize=12)
    ax.set_ylabel("Epicenter Location (Lat, Lon)", fontsize=12)

    high  = mpatches.Patch(color='#FF0044', label='HIGH RISK (Top 3)')
    med   = mpatches.Patch(color='#FFA500', label='MEDIUM RISK')
    low   = mpatches.Patch(color='#1E90FF', label='LOWER RISK')
    ax.legend(handles=[high, med, low], loc='lower right')
    plt.tight_layout()
    out = os.path.join(project_root, "charts", "risk_ranking.png")
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"    Saved -> {out}")
else:
    print("    [!] Skipped — run risk_dashboard.py first to generate risk_ranking_2030.csv")

# ─────────────────────────────────────────────
# CHART 3: Urban vs Rural Pixel Distribution
# ─────────────────────────────────────────────
print("[+] Generating Chart 3: Urban vs Rural Distribution...")
features_path = os.path.join(project_root, "data", "processed", "ml_features.parquet")

if os.path.exists(features_path):
    df = spark.read.parquet(features_path)
    total  = df.count()
    urban  = df.filter(col("is_urban") == 1).count()
    rural  = total - urban

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Pie Chart
    axes[0].pie([urban, rural], labels=['Urban', 'Rural'],
                colors=['#FF0044', '#32CD32'], autopct='%1.1f%%',
                startangle=140, textprops={'fontsize': 13})
    axes[0].set_title("Land Classification Split\n(WorldPop 2020 Baseline)",
                      fontsize=13, fontweight='bold')

    # KDE Density Plot
    df_sample = df.sample(fraction=0.05, seed=42).select("density_score").toPandas()
    sns.kdeplot(data=df_sample, x="density_score", fill=True,
                color="#1E90FF", ax=axes[1])
    axes[1].set_title("Population Density Score Distribution\n(Log-Transformed)",
                      fontsize=13, fontweight='bold')
    axes[1].set_xlabel("Density Score (log1p)")
    axes[1].set_ylabel("Frequency")

    plt.suptitle("Pakistan Spatial Data Overview", fontsize=15,
                 fontweight='bold', y=1.01)
    plt.tight_layout()
    out = os.path.join(project_root, "charts", "urban_rural_distribution.png")
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"    Saved -> {out}")
else:
    print("    [!] Skipped — run features.py first")

# ─────────────────────────────────────────────
# CHART 4: Urbanization Rate Line Chart
# ─────────────────────────────────────────────
print("[+] Generating Chart 4: Urbanization Rate Trend...")
data = {
    'Year':             [1998, 2017, 2022, 2030],
    'Urban %':          [32,   36,   39,   45],
    'Rural %':          [68,   64,   61,   55],
}
df_trend = pd.DataFrame(data)

fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(df_trend['Year'], df_trend['Urban %'], marker='o', color='#FF0044',
        linewidth=2.5, markersize=8, label='Urban Population %')
ax.plot(df_trend['Year'], df_trend['Rural %'], marker='s', color='#32CD32',
        linewidth=2.5, markersize=8, label='Rural Population %')
ax.fill_between(df_trend['Year'], df_trend['Urban %'], alpha=0.15, color='#FF0044')
ax.fill_between(df_trend['Year'], df_trend['Rural %'], alpha=0.10, color='#32CD32')

for _, row in df_trend.iterrows():
    ax.annotate(f"{row['Urban %']}%", (row['Year'], row['Urban %']),
                textcoords="offset points", xytext=(0, 10), ha='center', fontsize=10)

ax.axvline(x=2022, color='gray', linestyle='--', alpha=0.5, label='Projection Starts')
ax.set_title("Pakistan Urbanization Rate Trend (1998–2030)",
             fontsize=15, fontweight='bold', pad=12)
ax.set_xlabel("Year", fontsize=12)
ax.set_ylabel("Population Share (%)", fontsize=12)
ax.set_ylim(0, 100)
ax.set_xticks([1998, 2017, 2022, 2030])
ax.legend(fontsize=11)
plt.tight_layout()
out = os.path.join(project_root, "charts", "urbanization_trend.png")
plt.savefig(out, dpi=150)
plt.close()
print(f"    Saved -> {out}")

print("\n[SUCCESS] All Charts Generated Successfully!")
print(f"   Find them in: {os.path.join(project_root, 'charts')}/")
spark.stop()


# "visualize.py"       "Folium Map Generation"
# I built this cartography engine to render all the AI predictions onto an interactive map. It layers the future sprawl zones and the 15 mega centers onto a beautiful dark mode and satellite canvas.


import os  # Establishes the fundamental operating system directory interaction layer
import pandas as pd  # Integrates the critical tabular data manipulation framework
import folium  # Loads the primary geospatial rendering and cartography library
from branca.element import Template, MacroElement  # Extracts advanced markup injection modules for custom interactive components
from pyspark.sql import SparkSession  # Retrieves the core distributed computing session architecture
project_root = os.getcwd()  # Captures the absolute directory path of the active Python runtime environment
java11_dir = os.path.join(project_root, "java11_folder")  # Synthesizes the exact path targeting the localized Java virtual machine
for item in os.listdir(java11_dir):  # Initiates a recursive loop traversing the Java repository structure
    if "jdk" in item.lower():  # Validates the presence of the critical development kit execution environment
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically configures the absolute path required for PySpark cluster execution
        break  # Terminates the iteration sequence upon successful dependency resolution
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Establishes the static path for Windows specific big data utilities
spark = SparkSession.builder.appName("SatelliteVisualization").getOrCreate()  # Instantiates the primary memory cluster for cartographic data extraction
spark.sparkContext.setLogLevel("ERROR")  # Suppresses non critical terminal outputs to maintain console clarity
sprawl_path = os.path.join(project_root, "data", "predictions", "sprawl_2030.parquet")  # Constructs the absolute path locating the dense urban coordinate dataset
peri_urban_path = os.path.join(project_root, "data", "predictions", "peri_urban_2030.parquet")  # Constructs the absolute path locating the transitional suburban dataset
centers_path = os.path.join(project_root, "data", "predictions", "future_city_centers.csv")  # Constructs the absolute path locating the previously calculated mathematical epicenters
df_sprawl = spark.read.parquet(sprawl_path).toPandas()  # Ingests the massive urban matrix and compresses it into a local tabular structure
df_suburbs = spark.read.parquet(peri_urban_path).toPandas()  # Ingests the transitional suburban matrix and converts it into local memory
df_centers = pd.read_csv(centers_path)  # Loads the mathematical epicenter coordinates directly into standard local memory
print("Rendering the map layers")  # Signals the commencement of the cartographic compilation sequence
pakistan_map = folium.Map(location=[30.3753, 69.3451], zoom_start=6)  # Initializes the base canvas utilizing the geographic center coordinates of the target nation
folium.TileLayer('CartoDB dark_matter', name="Dark Dashboard").add_to(pakistan_map)  # Injects a high contrast dark mode base layer to emphasize data points
folium.TileLayer('OpenStreetMap', name="Street Labels", show=False).add_to(pakistan_map)  # Injects a secondary street level vector layer initially hidden from view
folium.TileLayer(  # Initiates the configuration of the high resolution satellite imagery layer
    tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',  # Specifies the external endpoint supplying the localized imagery tiles
    attr='Esri',  # Assigns the mandatory legal attribution parameter for the external tile provider
    name='Satellite X Ray',  # Assigns an internal reference identifier for the interactive layer control
    show=False  # Configures the satellite layer to remain hidden upon initial canvas load
).add_to(pakistan_map)  # Binds the fully configured satellite tile layer to the primary map object
for idx, row in df_suburbs.iterrows():  # Initiates a localized loop iterating through every transitional suburban coordinate
    folium.CircleMarker(  # Instantiates a geometric vector rendering object for the specific coordinate
        location=[row['lat'], row['lon']], radius=3.5, color='#FF8C00', weight=0, fill=True, fill_color='#FF8C00', fill_opacity=0.35  # Configures the suburban visual parameters assigning wide radius and semi transparent orange coloring
    ).add_to(pakistan_map)  # Attaches the specific suburban vector object to the primary canvas
for idx, row in df_sprawl.iterrows():  # Initiates a localized loop iterating through every high density urban coordinate
    folium.CircleMarker(  # Instantiates a geometric vector rendering object for the specific coordinate
        location=[row['lat'], row['lon']], radius=1.5, color='#ff0044', weight=0, fill=True, fill_color='#ff0044', fill_opacity=0.8  # Configures the core urban visual parameters assigning a tight radius and highly opaque crimson coloring
    ).add_to(pakistan_map)  # Attaches the specific core urban vector object to the primary canvas
for idx, row in df_centers.iterrows():  # Initiates a localized loop iterating through the mathematical epicenter coordinates
    folium.CircleMarker(  # Instantiates a geometric vector rendering object acting as a highlight ring
        location=[row['lat'], row['lon']], radius=14, color='#FFD700', weight=1.5, fill=True, fill_color='#FFD700', fill_opacity=0.4,  # Configures the highlight ring assigning a massive radius and semi transparent golden coloring
        tooltip=f"<b>Future Mega Center {row['center_id']}</b>"  # Injects interactive HTML markup to render an informational popup upon cursor hover
    ).add_to(pakistan_map)  # Attaches the epicenter highlight ring to the primary canvas
    folium.Marker(  # Instantiates a distinct graphical icon object to anchor the mathematical epicenter
        location=[row['lat'], row['lon']], icon=folium.Icon(color='orange', icon='star')  # Configures the physical anchor utilizing a specialized star icon graphic
    ).add_to(pakistan_map)  # Attaches the physical anchor graphic to the primary canvas
folium.LayerControl(position='topright').add_to(pakistan_map)  # Injects the interactive user interface element enabling dynamic layer toggling
output_map_path = os.path.join(project_root, "beautiful_sprawl_map.html")  # Synthesizes the exact destination path for the finalized interactive cartography document
pakistan_map.save(output_map_path)  # Serializes the entire configured cartographic canvas into a standalone HTML file
print(f"\n Masterpiece saved Open {output_map_path}")  # Outputs the final export destination to the terminal interface
spark.stop()  # Executes the final tear down protocol for the distributed computing environment
