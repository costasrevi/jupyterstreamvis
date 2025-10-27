import tensorwatchext as tw
from .stream_unit import StreamUnit
from tensorwatchext import kafka_connector as kc
from tensorwatchext import pykafka_connector as pyc
from IPython.display import display
import asyncio
from ipywidgets import widgets
import time


# ----------------------------- TWAPI CONTROLLER -----------------------------
class twapi:
    """Main controller for connector + multiple stream/draw units."""

    def __init__(self):
        self.loop = asyncio.get_event_loop() # Capture the main event loop
        self.client = tw.WatcherClient()
        self.connector_instance = None
        self.units = {}
        self.apply_enabled = False
        self.metrics_label = widgets.Label(value="")

    def update_metrics(self, metrics: str):
        self.metrics_label.value = metrics

    def enable_apply_button(self):
        # Automatically trigger the first plot draw when the connector is ready.
        self.apply_with_debounce()
        # Schedule enabling of individual unit buttons on the main thread
        for unit in self.units.values():
            self.loop.call_soon_threadsafe(unit.enable_apply_button)

    def apply_with_debounce(self, *args, **kwargs):
        # Schedule apply_with_debounce for each unit on the main thread
        for unit in self.units.values():
            self.loop.call_soon_threadsafe(unit.apply_with_debounce)

    def connector(self, topic, host, conn_type="kafka", **kwargs):
        if self.connector_instance:
            print("Connector already exists — reusing it.")
            return self.connector_instance

        if conn_type == "kafka":
            self.connector_instance = kc(topic=topic, hosts=host, twapi_instance=self, **kwargs)
        elif conn_type == "pykafka":
            self.connector_instance = pyc(topic=topic, hosts=host, twapi_instance=self, **kwargs)
        else:
            raise ValueError("Invalid connector type: choose 'kafka' or 'pykafka'")
        # print("Connector created.")
        return self.connector_instance

    def add_unit(self,expr, name: str = "default"):
        if name in self.units:
            print(f"Unit '{name}' already exists.")
            return self.units[name]
        unit = StreamUnit(name, expr, self.client)
        self.units[name] = unit
        # print(f"Stream unit '{name}' added.")
        return unit

    def draw(self, name: str = "default"):
        if name not in self.units:
            print(f"Unit '{name}' not found.")
            return
        unit = self.units[name]
        unit.draw()

    def draw_all(self):
        all_uis = [unit.ui for unit in self.units.values()]
        display(widgets.VBox(all_uis))

    def defer_apply(self):
        """Backward-compatible fallback for connectors expecting defer_apply()."""
        # Schedule apply_with_debounce on the main thread to ensure UI updates are safe.
        self.loop.call_soon_threadsafe(self.apply_with_debounce)