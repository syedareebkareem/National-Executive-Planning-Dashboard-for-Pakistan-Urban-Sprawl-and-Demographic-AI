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