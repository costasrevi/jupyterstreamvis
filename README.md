# jupyter-stream-vis
This is a project for my thesis at Technical University of Crete.
Our goal is to make to make it simple to visualize streaming data on jupyter Lab
This projects main goal is to provide a connector to Kafka and with the help of some parsers and a simple to use 
api provide the user the abillity to stream his streaming data.

jupyterstreamvis.twapi: Interactive Kafka Stream Visualization
twapi is a high-level Python API designed to simplify real-time data visualization from Apache Kafka streams directly within Jupyter notebooks. It wraps the powerful tensorwatch library and provides an interactive UI built with ipywidgets to control visualization parameters on the fly.

This tool is perfect for data scientists, engineers, and analysts who need to quickly inspect, monitor, and visualize streaming data without leaving their development environment.

Features
Interactive UI: Control plot parameters like window size, colors, and more in real-time.
Chainable API: A fluent, easy-to-read API for connecting, defining a stream, and drawing.
Dual Kafka Backends: Supports both confluent-kafka (default) and pykafka for flexible connectivity.
Live Metrics: Display real-time stream metrics like throughput and latency alongside your visualization.
Custom Processing: Use simple or complex Python lambda expressions to process and aggregate data before plotting.
Efficient Updates: Uses a debouncing mechanism to ensure a smooth user experience and prevent the kernel from being overwhelmed by rapid UI changes.
Installation
Before using twapi, ensure you have the necessary libraries installed.

Install the core package and its dependencies:

bash
pip install jupyterstreamvis # Or the name of your package
pip install tensorwatch ipywidgets matplotlib
Install a Kafka client library:

bash
# For the default 'kafka' connector
pip install confluent-kafka

# Or for the 'pykafka' connector
pip install pykafka
Enable ipywidgets in Jupyter:

bash
jupyter nbextension enable --py widgetsnbextension
Quick Start
Visualizing a Kafka stream is as simple as a few lines of code. The example below connects to a Kafka topic, calculates a running sum of the seq field from incoming messages, and plots the result.

python
# 1. Enable the interactive matplotlib backend in your Jupyter notebook
%matplotlib widget

# 2. Import the library
from jupyterstreamvis import twapi as tw

# 3. Initialize the twapi instance
test = tw()

# 4. Create a connector to your Kafka stream
# This connector also reports benchmark metrics.
test.connector(
    topic='presentation',
    host='localhost:9093',
    cluster_size=10,
    queue_length=200000
)

# 5. Define the stream processing logic
# This lambda calculates the sum of the 'seq' field from all messages in the current data window.
test.stream(expr='lambda d: sum(msg["seq"] for msg in d.data) if d.data else 0')

# 6. Draw the UI and start the visualization
test.draw_with_metrics()
API Reference
twapi()
Initializes the API wrapper and all the UI components.

.connector(...)
Creates and starts a Kafka consumer in a background thread. This method is responsible for fetching data and calculating benchmark metrics.

Arguments:

topic (str): The Kafka topic to consume from.
host (str): The Kafka broker host and port (e.g., 'localhost:9092').
conn_type (str, optional): The connector library to use. Can be 'kafka' (default, for confluent-kafka) or 'pykafka'.
parsetype (str, optional): The message format (e.g., 'json', 'pickle', 'avro'). Defaults to 'json'.
queue_length (int, optional): The maximum number of messages to hold in memory. Defaults to 50000.
cluster_size (int, optional): The number of consumer threads to spawn. Defaults to 1.
...: Other parameters specific to the chosen conn_type.
.stream(expr)
Defines the data processing logic that will be applied to the stream.

Arguments:

expr (str): A string containing a Python lambda expression. The lambda receives a data object d (which is the connector instance) and should return a single numerical value to be plotted. The raw messages are available in d.data.
.draw() or .draw_with_metrics()
Renders the interactive UI widgets and the plot area in the Jupyter cell.

.draw(): Renders the standard UI.
.draw_with_metrics(): Renders the UI with an additional label for displaying benchmark statistics.
UI Controls
Reset Button: Resets all visualization options to their default values and clears the plot.
Start / Apply Changes Button: Initially labeled "Start". It becomes active once the first Kafka message is received. Clicking it begins the visualization. Afterward, it's used to apply any changes made in the options.
Visualization Options (Accordion):
Window Size: Controls the number of data points displayed on the plot at one time.
Window Width: Adjusts the physical width of the plot.
Pick a Color: Changes the color of the plot line.
Date / Use Offset / Dim History: Checkboxes to control other tensorwatch visualizer features.
