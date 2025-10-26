from .stream_unit import StreamUnit
from ipywidgets import widgets
import tensorwatchext as tw
from tensorwatchext import kafka_connector as kc
from tensorwatchext import pykafka_connector as pyc


class twapi:
    """Controller for connector + multiple StreamUnits."""

    def __init__(self):
        self.client = tw.WatcherClient()
        self.connector_instance = None
        self.units = {}
        self.apply_enabled = False
        self.metrics_label = widgets.Label(value="")

    def update_metrics(self, metrics: str):
        self.metrics_label.value = metrics

    def enable_apply_button(self):
        """Enable all units' Apply buttons."""
        for unit in self.units.values():
            unit.enable_apply_button()
        print("✅ Global apply enable: all stream units now active.")

    def apply_with_debounce(self, *args, **kwargs):
        for unit in self.units.values():
            unit.apply_with_debounce()

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
        print("Connector created.")
        return self.connector_instance

    def add_unit(self, name, expr):
        if name in self.units:
            print(f"Unit '{name}' already exists.")
            return self.units[name]
        unit = StreamUnit(name, expr, self.client)
        self.units[name] = unit
        print(f"Stream unit '{name}' added.")
        return unit

    def defer_apply(self):
        """Backward-compatible fallback for connectors expecting defer_apply()."""
        print("⚙️ Deferred apply requested by connector.")
        self.apply_with_debounce()  # trigger apply button on first message

    def draw(self, name):
        if name not in self.units:
            print(f"Unit '{name}' not found.")
            return
        self.units[name].draw()

    def draw_all(self):
        all_uis = [unit.ui for unit in self.units.values()]
        from IPython.display import display
        display(widgets.VBox(all_uis))
