import tensorwatchext as tw
from tensorwatchext import kafka_connector as kc
from tensorwatchext import pykafka_connector as pyc
from IPython.display import display
from ipywidgets import widgets
import time
import logging
import matplotlib.pyplot as plt

class twapi:
    """TensorWatch API Wrapper for Kafka Streaming and Visualization"""

    def __init__(self):
        """Initializes the twapi class, setting up the UI widgets and event handlers."""
        self.default_value = 10
        self.visualizer = None  # Initialize visualizer as None
        self.client = tw.WatcherClient()
        self.out = widgets.Output(layout={})

        # Initialize UI widgets
        self.update_interval = 0.5  # Delay in seconds
        self.my_slider = widgets.IntSlider(value=self.default_value, min=1, max=100, step=1, description="Window Size:")
        self.my_slider2 = widgets.IntSlider(value=self.default_value, min=1, max=100, step=1, description="Window Width:")
        self.datebutton = widgets.Checkbox(value=False, description="Date")
        self.offsetbutton = widgets.Checkbox(value=False, description="Use Offset")
        self.dimhistorybutton = widgets.Checkbox(value=True, description="Dim History")
        self.colorpicker = widgets.ColorPicker(value="blue", description="Pick a Color")
        
        self.button_reset = widgets.Button(description="Reset", tooltip="Reset stream settings")        
        self.button_apply = widgets.Button(description="Please wait", tooltip="Apply changes to the visualization", disabled=True)

        # Group widgets for a cleaner UI
        left_box = widgets.VBox([self.my_slider, self.my_slider2, self.colorpicker])
        right_box = widgets.VBox([self.offsetbutton, self.dimhistorybutton, self.datebutton])
        self.options_box = widgets.HBox([left_box, right_box])
        self.accordion = widgets.Accordion(children=[self.options_box])
        self.accordion.set_title(0, 'Visualization Options')

        # Event handlers
        self._last_update = time.time()
        self.button_reset.on_click(self.reset)
        self.button_apply.on_click(self.apply_with_debounce)
        self.metrics_label = widgets.Label(value="")

        # Observe widget changes directly
        self.my_slider.observe(self.apply_with_debounce, names='value')
        self.my_slider2.observe(self.apply_with_debounce, names='value')
        self.colorpicker.observe(self.apply_with_debounce, names='value')

    def stream(self, expr):
        """Creates a TensorWatch stream from an expression."""
        self.expr = expr
        try:
            self.streamdata = self.client.create_stream(expr=expr)
            logging.debug("Stream created successfully")
        except Exception as e:
            logging.error(f"Error creating stream: {e}")
            print(f"Error creating stream: {e}")
        return self

    def apply_with_debounce(self, _=None):
        """Debounced apply function to prevent too frequent updates."""
        now = time.time()
        if now - self._last_update > self.update_interval:
            self.update_visualizer()
            self._last_update = now
            if self.button_apply.description == "Start":
                self.button_apply.description = "Apply Changes"

    def update_visualizer(self, _=None):
        """Updates the TensorWatch visualizer with the latest widget values."""
        if not hasattr(self, 'streamdata') or not self.streamdata:
            self.out.clear_output(wait=True)
            with self.out:
                print("Stream data not available or empty yet. Please wait for data.")
            return

        try:
            # Always clear output before drawing
            self.out.clear_output(wait=True)

            # Close previous visualizer if it exists to free resources
            if self.visualizer:
                # self.visualizer.close()
                try:    
                    plt.pause(0.05)
                    plt.close('all') # Also close any lingering matplotlib figures
                except Exception:
                    pass
            # Create a new visualizer with the current settings
            self.visualizer = tw.Visualizer(
                self.streamdata,
                vis_type="line",
                window_width=self.my_slider2.value,
                window_size=self.my_slider.value,
                Date=self.datebutton.value,
                useOffset=self.offsetbutton.value,
                dim_history=self.dimhistorybutton.value,
                color=self.colorpicker.value,
            )
            with self.out:
                self.visualizer.show()
                
        except Exception as e:
            self.out.clear_output(wait=True)
            with self.out:
                print(f"Error updating visualizer: {e}")

    def enable_apply_button(self):
        """Enables the apply button and changes its description to 'Start'."""
        logging.debug("Enabling apply button.")
        self.button_apply.disabled = False
        self.button_apply.description = "Start"

    def reset(self, _=None):
        """Resets all widget values to their defaults and clears the visualization."""
        self.my_slider.value = self.default_value
        self.my_slider2.value = self.default_value
        self.datebutton.value = False
        self.offsetbutton.value = False
        self.dimhistorybutton.value = True
        self.colorpicker.value = "blue"
        
        # Clear the output and close the visualizer
        self.out.clear_output()
        plt.close('all')
        self.visualizer = None

    def draw(self):
        """Displays the UI for controlling the visualization."""
        ui = widgets.VBox([
            widgets.HBox([self.button_reset, self.button_apply]),
            self.accordion,
            self.out
        ])
        display(ui)

    def draw_with_metrics(self):
        """Displays the UI for controlling the visualization with a metrics label."""
        ui = widgets.VBox([
            self.metrics_label,
            widgets.HBox([self.button_reset, self.button_apply]),
            self.accordion,
            self.out
        ])
        display(ui)

    def update_metrics(self, metrics):
        """Updates the metrics label with the provided text."""
        self.metrics_label.value = metrics

    def connector(self, topic, host, parsetype="json", cluster_size=1, conn_type="kafka", queue_length=50000,
                  group_id="mygroup", schema_path=None, protobuf_message=None, parser_extra=None,
                  random_sampling=None, countmin_width=None, countmin_depth=None, ordering_field=None):
        """
        Creates and returns a Kafka or PyKafka connector.

        Args:
            topic (str): The Kafka topic to consume from.
            host (str): The Kafka broker host.
            parsetype (str): The message format (e.g., 'json', 'pickle', 'avro').
            cluster_size (int): The number of consumer threads.
            conn_type (str): The type of connector to use ('kafka' or 'pykafka').
            queue_length (int): The maximum size of the message queue.
            group_id (str): The Kafka consumer group ID.
            schema_path (str): The path to the schema file.
            protobuf_message (str): The name of the Protobuf message class.
            parser_extra (str): Extra data for the parser (e.g., Avro schema for 'pykafka').
            random_sampling (int): The percentage of messages to sample.
            countmin_width (int): The width of the Count-Min Sketch.
            countmin_depth (int): The depth of the Count-Min Sketch.

        Returns:
            A KafkaConnector or pykafka_connector instance.
        """
        if conn_type == "kafka":
            return kc(
                topic=topic, hosts=host, parsetype=parsetype, cluster_size=cluster_size, queue_length=queue_length, group_id=group_id,
                 schema_path=schema_path, protobuf_message=protobuf_message,parser_extra=parser_extra,
                random_sampling=random_sampling, countmin_width=countmin_width,ordering_field=ordering_field,
                countmin_depth=countmin_depth,
                twapi_instance=self)
        elif conn_type == "pykafka":
            return pyc(
                topic=topic, hosts=host, parsetype=parsetype, cluster_size=cluster_size,twapi_instance=self, 
                queue_length=queue_length, consumer_group=bytes(group_id, 'utf-8'),
                parser_extra=parser_extra, schema_path=schema_path, protobuf_message=protobuf_message,
                random_sampling=random_sampling, countmin_width=countmin_width,ordering_field=ordering_field,
                countmin_depth=countmin_depth)
        else:
            raise ValueError("Invalid connector type. Choose 'kafka' or 'pykafka'.")
