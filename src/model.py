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