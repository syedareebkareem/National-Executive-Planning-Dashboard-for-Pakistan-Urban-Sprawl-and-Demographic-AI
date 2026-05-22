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