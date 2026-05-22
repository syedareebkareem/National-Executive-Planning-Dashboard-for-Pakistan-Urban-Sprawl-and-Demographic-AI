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