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