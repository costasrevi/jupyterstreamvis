# jupyter-stream-vis

This project is designed to simplify the visualization of streaming data from Apache Kafka directly within Jupyter Lab. It provides a simple-to-use API that wraps powerful streaming and plotting libraries, allowing users to quickly connect to a Kafka topic and visualize data with an interactive UI.

This tool is perfect for data scientists, engineers, and analysts who need to quickly inspect, monitor, and visualize streaming data without leaving their development environment.

## Features

- **Interactive UI**: Control plot parameters like window size, colors, and more in real-time using `ipywidgets`.
- **Direct-to-Draw Expressions**: Define your data processing logic as a lambda expression and pass it directly to the draw call.
- **Dual Kafka Backends**: Supports both `confluent-kafka` (default) and `pykafka` for flexible connectivity.
- **Live Metrics**: Display real-time stream metrics like throughput and latency alongside your visualization.
- **Efficient Updates**: Uses a debouncing mechanism to ensure a smooth user experience and prevent the kernel from being overwhelmed by rapid UI changes.

## Installation

Before using, ensure you have the necessary libraries installed.

1.  **Install Core Libraries**:
    ```bash
    pip install ipywidgets matplotlib
    ```

2.  **Install a Kafka Client**:
    ```bash
    # For the default 'kafka' connector
    pip install confluent-kafka

    # Or for the 'pykafka' connector
    pip install pykafka
    ```
    
3. **Install the Jupyter Matplotlib Backend**:
    For interactive plotting in JupyterLab, you need `ipympl`.
    ```bash
    pip install ipympl
    ```

4.  **Enable Widgets in Jupyter**:
    ```bash
    jupyter nbextension enable --py widgetsnbextension
    ```

## Quick Start

The example below connects to a Kafka topic, calculates the processing latency for messages, and plots the result.

```python
# 1. Enable the interactive matplotlib backend in your Jupyter notebook
# This MUST be in the first cell or at the top of your script.
%matplotlib widget

# 2. Import the library and other necessary modules
from jupyterstreamvis import twapi as tw
import time

# 3. Initialize the twapi instance
demo = tw()

# 4. Create a connector to your Kafka stream
# The connector will start fetching messages in the background.
# The 'Apply' button will become enabled once the first message is received.
demo.connector(
    topic='presentation-demo-s1', 
    host='192.168.68.110:9092',
    cluster_size=3,
    parsetype="json"
)

# 5. Draw the UI and provide the stream processing logic
# The lambda expression calculates the time difference since the message was sent.
# The plot will start once you click the 'Start' button.
demo.draw_with_metrics(expr='lambda d: time.time() - d.data[-1]["send_time"] if d.data else 0')

```

## API Reference

### `twapi()`

Initializes the API wrapper and all the UI components.

### `.connector(...)`

Creates and starts a Kafka consumer in a background thread. This method is responsible for fetching data.

**Arguments**:

-   `topic` (str): The Kafka topic to consume from.
-   `host` (str): The Kafka broker host and port (e.g., 'localhost:9092').
-   `conn_type` (str, optional): The connector library to use. Can be `'kafka'` (default, for `confluent-kafka`) or `'pykafka'`.
-   `parsetype` (str, optional): The message format (e.g., 'json', 'pickle', 'avro'). Defaults to 'json'.
-   `...`: Other parameters specific to the chosen `conn_type`.

### `.draw(expr)` or `.draw_with_metrics(expr)`

Renders the interactive UI widgets and the plot area in the Jupyter cell. This is the final step and should be called after the connector is configured.

**Arguments**:

-   `expr` (str): A string containing a Python lambda expression. The lambda receives a data object `d` (which is the connector instance) and should return a single numerical value to be plotted. The raw messages are available in `d.data`.

## UI Controls

-   **Reset Button**: Resets all visualization options to their default values and clears the plot.
-   **Start / Apply Changes Button**: Initially labeled "Please wait". It becomes active and changes to "Start" once the first Kafka message is received. Clicking it begins the visualization. Afterward, it's used to apply any changes made in the options.
-   **Visualization Options (Accordion)**:
    -   **Window Size**: Controls the number of data points displayed on the plot.
    -   **Window Width**: Adjusts the physical width of the plot.
    -   **Pick a Color**: Changes the color of the plot line.
    -   **Date / Use Offset / Dim History**: Checkboxes to control other visualizer features.