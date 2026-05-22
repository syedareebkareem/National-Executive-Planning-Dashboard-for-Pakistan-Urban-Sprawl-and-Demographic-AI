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