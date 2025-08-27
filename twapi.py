import tensorwatch as tw
from . import kafka_connector as kc
from . import pykafka_connector as pyc
from IPython.display import display
from ipywidgets import widgets
import asyncio


class twapi:
    """TensorWatch API Wrapper for Kafka Streaming and Visualization"""

    def __init__(self):
        self.default_value = 10
        self.visualizer = None  # Initialize visualizer as None
        self.client = tw.WatcherClient()
        self.out = widgets.Output(layout={})

        # Initialize UI widgets
        self.my_slider = widgets.IntSlider(value=self.default_value, min=1, max=100, step=1, description="Window Size:")
        self.my_slider2 = widgets.IntSlider(value=self.default_value, min=1, max=100, step=1, description="Window Width:")
        self.datebutton = widgets.Checkbox(value=False, description="Date")
        self.offsetbutton = widgets.Checkbox(value=False, description="Use Offset")
        self.dimhistorybutton = widgets.Checkbox(value=True, description="Dim History")
        self.colorpicker = widgets.ColorPicker(value="blue", description="Pick a Color")
        
        self.button_reset = widgets.Button(description="Reset", tooltip="Reset stream settings")
        self.button_apply = widgets.Button(description="Apply", tooltip="Apply changes to the stream")

        # Organize widgets in a tab
        self.tab = widgets.Tab(children=[
            self.my_slider, self.my_slider2, self.datebutton, self.offsetbutton, self.dimhistorybutton, self.colorpicker
        ])
        tab_titles = ["Window Size", "Window Width", "Date Format", "Offset", "Dim History", "Color Picker"]
        for i, title in enumerate(tab_titles):
            self.tab.set_title(i, title)

        # Event handlers
        self.button_reset.on_click(self.reset)
        self.button_apply.on_click(self.update_visualizer)

    def stream(self, expr):
        """Creates a TensorWatch stream from an expression."""
        self.expr = expr
        self.streamdata = self.client.create_stream(expr=expr)
        return self

    def update_visualizer(self, _=None):
        """Updates the TensorWatch visualizer with the latest widget values."""
        try:
            if self.visualizer is None:
                # Create visualizer only if it doesn't exist
                self.out.clear_output()
                self.visualizer = tw.Visualizer(
                    self.streamdata,
                    vis_type="line",
                    window_width=self.my_slider.value,
                    window_size=self.my_slider2.value,
                    Date=self.datebutton.value,
                    useOffset=self.offsetbutton.value,
                    dim_history=self.dimhistorybutton.value,
                    color=self.colorpicker.value,
                )
                with self.out:
                    self.visualizer.show()
            else:
                # Update existing visualizer properties
                await self._async_update_visualizer()
        except Exception as e:
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

    async def _async_update_visualizer(self):
        """Asynchronously updates the visualizer properties."""
        try:
            # Perform updates in a non-blocking manner
            self.visualizer.window_width = self.my_slider.value
            self.visualizer.window_size = self.my_slider2.value
        except Exception as e:
            print(f"Async error updating visualizer: {e}")

    def draw(self):
        """Displays the UI for controlling the visualization."""
        display(self.button_reset, self.button_apply, self.out, self.tab)

    def connector(self, topic, host, parsetype="json", cluster_size=1, conn_type="kafka"):
        """Returns a Kafka or PyKafka connector."""
        if conn_type == "kafka":
            return kc.kafka_connector(topic=topic, hosts=host, parsetype=parsetype, cluster_size=cluster_size)
        elif conn_type == "pykafka":
            return pyc.pykafka_connector(topic=topic, hosts=host, parsetype=parsetype, cluster_size=cluster_size)
        else:
            raise ValueError("Invalid connector type. Choose 'kafka' or 'pykafka'.")

    async def some_async_function(self):
        """Example of an async function that can be called."""
        await asyncio.sleep(1)
        print("Async function completed")
