import tensorwatch as tw
from . import kafka_connector as kc
from . import pykafka_connector as pyc
from IPython.display import display
from ipywidgets import widgets
import asyncio
import time


class twapi:
    """TensorWatch API Wrapper for Kafka Streaming and Visualization"""

    def __init__(self):
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
        self.button_apply = widgets.Button(description="Apply Changes", tooltip="Apply changes to the visualization")

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
            # Initialize the visualizer immediately after stream creation
            # self.update_visualizer()
        except Exception as e:
            print(f"Error creating stream: {e}")
        return self

    def apply_with_debounce(self, _=None):
        """Debounced apply function to prevent too frequent updates."""
        # now = time.time()
        # if now - self._last_update > self.update_interval:
        self.update_visualizer()
            # self._last_update = now

    def update_visualizer(self, _=None):
        """Updates the TensorWatch visualizer with the latest widget values."""
        try:
            # Always clear the output and recreate the visualizer to apply changes
            self.out.clear_output()
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
            with self.out:
                print(f"Error updating visualizer: {e}")

    def reset(self, _=None):
        """Resets all widget values to their defaults."""
        self.my_slider.value = self.default_value
        self.my_slider2.value = self.default_value
        self.datebutton.value = False
        self.offsetbutton.value = False
        self.dimhistorybutton.value = True
        self.colorpicker.value = "blue"
        
        # Clear the output and set visualizer to None
        self.out.clear_output()
        self.visualizer = None

    def draw(self):
        """Displays the UI for controlling the visualization."""
        self.update_visualizer()
        display(widgets.HBox([self.button_reset, self.button_apply]), self.accordion, self.out)

    def draw_with_metrics(self):
        """Displays the UI for controlling the visualization with metrics."""
        self.update_visualizer()
        display(self.metrics_label, widgets.HBox([self.button_reset, self.button_apply]), self.accordion, self.out)

    def update_metrics(self, metrics):
        """Updates the metrics label."""
        self.metrics_label.value = metrics

    def connector(self, topic, host, parsetype="json", cluster_size=1, conn_type="kafka", queue_length=50000,
                  group_id="mygroup", avro_schema=None, schema_path=None, protobuf_message=None, parser_extra=None,
                  random_sampling=None, countmin_width=None, countmin_depth=None):
        """
        Returns a Kafka or PyKafka connector, exposing more configuration options.
        
        Args:
            parser_extra (str): For pykafka, this is used to pass the Avro schema string.
        """
        if conn_type == "kafka":
            return kc.KafkaConnector(
                topic=topic, hosts=host, parsetype=parsetype, cluster_size=cluster_size,
                twapi_instance=self, queue_length=queue_length, group_id=group_id,
                avro_schema=avro_schema, schema_path=schema_path, protobuf_message=protobuf_message,
                random_sampling=random_sampling, countmin_width=countmin_width,
                countmin_depth=countmin_depth)
        elif conn_type == "pykafka":
            # Note the mapping of parameters to pykafka's constructor
            return pyc.pykafka_connector(
                topic=topic, hosts=host, parsetype=parsetype, cluster_size=cluster_size,
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
