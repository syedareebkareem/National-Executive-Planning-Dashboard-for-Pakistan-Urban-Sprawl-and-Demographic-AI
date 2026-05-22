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