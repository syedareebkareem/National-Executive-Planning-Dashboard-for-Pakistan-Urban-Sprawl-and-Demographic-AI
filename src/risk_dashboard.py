import os  # Establishes the foundational operating system directory interaction layer
import pandas as pd  # Integrates the critical tabular data manipulation framework for final reporting
from pyspark.sql import SparkSession  # Extracts the core distributed computing session architecture
from pyspark.sql.functions import col, sum as _sum, count as _count, round as _round  # Ingests mathematical aggregation and precision functions for dataframe manipulation
from pyspark.ml.clustering import KMeans  # Imports the centroid based unsupervised spatial grouping algorithm
from pyspark.ml.feature import VectorAssembler  # Loads the matrix transformation utility required for spatial analysis
project_root = os.getcwd()  # Captures the absolute directory path of the active Python runtime environment
java11_dir = os.path.join(project_root, "java11_folder")  # Synthesizes the exact path targeting the localized Java virtual machine
for item in os.listdir(java11_dir):  # Initiates a recursive loop traversing the Java repository structure
    if "jdk" in item.lower():  # Validates the presence of the critical development kit execution environment
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)  # Dynamically configures the absolute path required for PySpark cluster execution
        break  # Terminates the iteration sequence upon successful dependency resolution
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Establishes the static path for Windows specific big data utilities
spark = SparkSession.builder.appName("RiskDashboard").getOrCreate()  # Instantiates the primary memory cluster for risk assessment calculations
spark.sparkContext.setLogLevel("ERROR")  # Suppresses non critical terminal outputs to maintain console clarity
print("\n" + "="*70)  # Generates a structural boundary for the terminal output interface
print("  EXECUTIVE DASHBOARD: 2030 SPRAWL RISK RANKING")  # Outputs the primary header for the final analytical sequence
print("="*70)  # Renders the lower structural boundary for visual separation
data_path = os.path.join(project_root, "data", "predictions", "sprawl_2030.parquet")  # Constructs the absolute path locating the previously generated future sprawl dataset
df = spark.read.parquet(data_path)  # Loads the compressed predictive spatial matrix into distributed cluster memory
assembler = VectorAssembler(inputCols=["lat", "lon"], outputCol="features")  # Configures the vectorization array specifically targeting the two dimensional geographic coordinates
df_features = assembler.transform(df)  # Executes the vector transformation generating the required spatial feature matrix
kmeans = KMeans().setK(15).setSeed(42).setFeaturesCol("features")  # Initializes the unsupervised algorithm with a locked seed forcing exact reproduction of the fifteen mathematical epicenters
model = kmeans.fit(df_features)  # Executes the training protocol recalculating the specific mathematical centers of gravity
predictions = model.transform(df_features)  # Deploys the trained model tagging every individual predicted coordinate with its nearest localized epicenter identifier
centers = model.clusterCenters()  # Extracts the precise geometric coordinates of the calculated fifteen epicenters
center_dict = {i: (round(centers[i][0], 4), round(centers[i][1], 4)) for i in range(15)}  # Synthesizes a rapid lookup dictionary mapping internal cluster identifiers to their truncated geometric locations
risk_df = predictions.groupBy("prediction").agg(_count("*").alias("sprawl_pixels"), _sum("future_population").alias("impacted_population")).toPandas()  # Aggregates spatial volume and demographic totals for each distinct epicenter transitioning the output to a local tabular format
risk_df["Epicenter_ID"] = risk_df["prediction"] + 1  # Standardizes the internal mathematical index converting a zero based array into a human readable identification sequence
risk_df["Latitude"] = risk_df["prediction"].apply(lambda x: center_dict[x][0])  # Extracts and appends the specific latitudinal coordinate associated with each categorized zone
risk_df["Longitude"] = risk_df["prediction"].apply(lambda x: center_dict[x][1])  # Extracts and appends the specific longitudinal coordinate associated with each categorized zone
risk_df = risk_df.sort_values(by="sprawl_pixels", ascending=False).reset_index(drop=True)  # Executes a descending mathematical sort prioritizing zones with the highest spatial density while resetting the tabular index
print(f"{'RANK':<5} | {'EPICENTER ID':<13} | {'LATITUDE':<10} | {'LONGITUDE':<10} | {'NEW SPRAWL AREA':<16} | {'IMPACTED POPULATION'}")  # Outputs the structured header matrix for the terminal risk report
print("-" * 85)  # Renders a strict horizontal boundary for tabular data separation
for index, row in risk_df.iterrows():  # Initiates a localized loop iterating through the sorted demographic risk matrix
    rank = index + 1  # Computes the sequential ranking integer based on the sorted array position
    epi_id = f"Center {int(row['Epicenter_ID'])}"  # Formats the epicenter identification string for clear terminal output
    lat = f"{row['Latitude']}"  # Extracts the formatted latitudinal value for tabular rendering
    lon = f"{row['Longitude']}"  # Extracts the formatted longitudinal value for tabular rendering
    area = f"{int(row['sprawl_pixels']):,} sq units"  # Formats the aggregate spatial volume utilizing standard numerical comma separation
    pop = f"{int(row['impacted_population']):,}"  # Formats the aggregate demographic impact variable matching the standardized structural layout
    if rank <= 3:  # Evaluates the sequential integer isolating the top three extreme hazard zones
        print(f"{rank:<5} | {epi_id:<13} | {lat:<10} | {lon:<10} | {area:<16} | {pop}  <-- HIGH RISK")  # Renders the tabular row injecting a critical visual alert flag
    else:  # Captures all standard hazard zones falling outside the primary critical threshold
        print(f"{rank:<5} | {epi_id:<13} | {lat:<10} | {lon:<10} | {area:<16} | {pop}")  # Outputs the standard tabular row structure without visual modifiers
print("-" * 85)  # Renders the terminal boundary finalizing the rendered risk report
out_path = os.path.join(project_root, "data", "predictions", "risk_ranking_2030.csv")  # Synthesizes the exact destination path for the localized tabular export document
risk_df.to_csv(out_path, index=False)  # Serializes the structured risk matrix to the local disk ensuring no index column is attached
print(f"[+] Full Executive Report saved to: {os.path.basename(out_path)}")  # Outputs the final export destination to the terminal interface
spark.stop()  # Executes the final tear down protocol for the distributed computing environment