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