import tensorwatch as tw
from . import kafka_connector as kc
from . import pykafka_connector as pyc
from IPython.display import display
from ipywidgets import widgets
import asyncio
import time
import logging
import matplotlib.pyplot as plt

class Vis:
    """A class to encapsulate a single visualization."""

    def __init__(self, client):
        """Initializes the Vis class."""
        self.default_value = 10
        self.visualizer = None  # Initialize visualizer as None
        self.title = None
        self.client = client
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
                self.visualizer.close()
                plt.close('all') # Also close any lingering matplotlib figures

            # Create a new visualizer with the current settings
            self.visualizer = tw.Visualizer(
                self.streamdata,
                vis_type="line",
                title=self.title,
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
        if self.visualizer:
            self.visualizer.close()
        plt.close('all')
        self.visualizer = None

    def draw(self, title=None):
        """Displays the UI for controlling the visualization."""
        self.title = title
        items = []
        if title:
            items.append(widgets.HTML(f"<h3>{title}</h3>"))
        
        items.extend([
            widgets.HBox([self.button_reset, self.button_apply]),
            self.accordion,
            self.out
        ])
        
        ui = widgets.VBox(items)
        display(ui)

    def draw_with_metrics(self, title=None):
        """Displays the UI for controlling the visualization with a metrics label."""
        self.title = title
        items = []
        if title:
            items.append(widgets.HTML(f"<h3>{title}</h3>"))
            
        items.extend([
            self.metrics_label,
            widgets.HBox([self.button_reset, self.button_apply]),
            self.accordion,
            self.out
        ])
        
        ui = widgets.VBox(items)
        display(ui)

    def update_metrics(self, metrics):
        """Updates the metrics label with the provided text."""
        self.metrics_label.value = metrics

class twapi:
    """TensorWatch API Wrapper for Kafka Streaming and Visualization"""

    def __init__(self):
        """Initializes the twapi class."""
        self.client = tw.WatcherClient()
        self.visualizations = []
        self.first_message_received = False

    def create_vis(self):
        """Creates and returns a Vis instance for a new visualization."""
        vis = Vis(self.client)
        self.visualizations.append(vis)
        return vis

    def enable_apply_buttons(self):
        """Enables the apply button on all visualizations."""
        for vis in self.visualizations:
            vis.enable_apply_button()

    def apply_with_debounce(self, _=None):
        """Applies changes to all visualizations with debounce."""
        for vis in self.visualizations:
            vis.apply_with_debounce()

    def update_metrics(self, metrics):
        """Updates the metrics on all visualizations."""
        for vis in self.visualizations:
            vis.update_metrics(metrics)

    def connector(self, topic, host, parsetype="json", cluster_size=1, conn_type="kafka", queue_length=50000,
                  group_id="mygroup", avro_schema=None, schema_path=None, protobuf_message=None, parser_extra=None,
                  random_sampling=None, countmin_width=None, countmin_depth=None):
        """
        Creates and returns a Kafka or PyKafka connector.
        """
        if conn_type == "kafka":
            return kc.KafkaConnector(
                topic=topic, hosts=host, parsetype=parsetype, cluster_size=cluster_size,
                twapi_instance=self, queue_length=queue_length, group_id=group_id,
                avro_schema=avro_schema, schema_path=schema_path, protobuf_message=protobuf_message,
                random_sampling=random_sampling, countmin_width=countmin_width,
                countmin_depth=countmin_depth)
        elif conn_type == "pykafka":
            return pyc.pykafka_connector(
                topic=topic, hosts=host, parsetype=parsetype, cluster_size=cluster_size,twapi_instance=self, 
                queue_length=queue_length, consumer_group=bytes(group_id, 'utf-8'),
                parser_extra=parser_extra, scema_path=schema_path, probuf_message=protobuf_message,
                random_sampling=random_sampling, countmin_width=countmin_width,
                countmin_depth=countmin_depth)
        else:
            raise ValueError("Invalid connector type. Choose 'kafka' or 'pykafka'.")

    async def some_async_function(self):
        """Example of an async function that can be called."""
        await asyncio.sleep(1)
        print("Async function completed")
