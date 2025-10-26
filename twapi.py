import tensorwatchext as tw
from tensorwatchext import kafka_connector as kc
from tensorwatchext import pykafka_connector as pyc
from IPython.display import display
from ipywidgets import widgets
import matplotlib.pyplot as plt
import time
import asyncio

# ----------------------------- STREAM UNIT -----------------------------
class StreamUnit:
    """A single stream + visualization unit connected to a shared connector."""

    def __init__(self, name, expr, client):
        self.name = name
        self.expr = expr
        self.client = client
        self.streamdata = self.client.create_stream(expr=expr)
        self.visualizer = None

        # Update control
        self.default_value = 10
        self._last_update = time.time()
        self.update_interval = 0.5  # seconds

        # --- Widgets (persistent references) ---
        self.out = widgets.Output()
        self.slider = widgets.IntSlider(value=self.default_value, min=1, max=100, description="Window Size:")
        self.width_slider = widgets.IntSlider(value=self.default_value, min=1, max=100, description="Window Width:")
        self.colorpicker = widgets.ColorPicker(value="blue", description="Color")
        self.dim_button = widgets.Checkbox(value=True, description="Dim History")
        self.date_button = widgets.Checkbox(value=False, description="Date")
        self.offset_button = widgets.Checkbox(value=False, description="Offset")

        self.button_reset = widgets.Button(description="Reset")
        self.button_apply = widgets.Button(
            description="Please wait",
            tooltip="Apply changes to this visualization",
            disabled=True
        )

        # Build UI
        self.build_ui()
        self.register_callbacks()

        # Initialize visualizer once
        with self.out:
            try:
                self.visualizer = tw.Visualizer(
                    self.streamdata,
                    vis_type="line",
                    window_width=int(self.width_slider.value),
                    window_size=int(self.slider.value),
                    Date=bool(self.date_button.value),
                    useOffset=bool(self.offset_button.value),
                    dim_history=bool(self.dim_button.value),
                    color=self.colorpicker.value,
                )
                self.visualizer.show()
            except Exception as e:
                print(f"Visualizer init error for '{self.name}':", e)

    def build_ui(self):
        left_box = widgets.VBox([self.slider, self.width_slider, self.colorpicker])
        right_box = widgets.VBox([self.dim_button, self.date_button, self.offset_button])
        options_box = widgets.HBox([left_box, right_box])
        self.accordion = widgets.Accordion(children=[options_box])
        self.accordion.set_title(0, f"{self.name} Options")

        self.ui = widgets.VBox([
            widgets.HBox([self.button_reset, self.button_apply]),
            self.accordion,
            self.out
        ])

    def register_callbacks(self):
        self.button_reset.on_click(self.reset)
        self.button_apply.on_click(self.apply_with_debounce)
        self.slider.observe(self.apply_with_debounce, names="value")
        self.width_slider.observe(self.apply_with_debounce, names="value")
        self.colorpicker.observe(self.apply_with_debounce, names="value")

    def draw(self):
        display(self.ui)

    def apply_with_debounce(self, _=None):
        now = time.time()
        if now - self._last_update > self.update_interval:
            self.update_visualizer()
            self._last_update = now

    def enable_apply_button(self):
        if self.button_apply.disabled:
            self.button_apply.disabled = False
            self.button_apply.description = "Apply"
            print(f"✅ Apply button enabled for '{self.name}'")

    def reset(self, _=None):
        """Reset UI controls (does not close visualizer)."""
        self.slider.value = self.default_value
        self.width_slider.value = self.default_value
        self.colorpicker.value = "blue"
        self.dim_button.value = True
        self.date_button.value = False
        self.offset_button.value = False

        # Refresh the visualizer with default values
        self.apply_with_debounce()

    async def _show_async(self):
        """Async-safe visualizer show inside Output widget."""
        with self.out:
            try:
                self.out.clear_output(wait=True)
                self.visualizer.show()
                plt.show(block=False)
            except Exception as e:
                print(f"Visualizer async error for '{self.name}':", e)

    def update_visualizer(self):
        """Update visualizer parameters in place (no new figure)."""
        if not self.visualizer:
            return

        try:
            # Update visualizer attributes
            self.visualizer.window_size = int(self.slider.value)
            self.visualizer.window_width = int(self.width_slider.value)
            self.visualizer.dim_history = bool(self.dim_button.value)
            self.visualizer.Date = bool(self.date_button.value)
            self.visualizer.useOffset = bool(self.offset_button.value)
            self.visualizer.color = self.colorpicker.value

            # Refresh display asynchronously
            loop = None
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            if loop.is_running():
                asyncio.ensure_future(self._show_async())
            else:
                loop.run_until_complete(self._show_async())

        except Exception as e:
            print(f"Visualizer update error for '{self.name}': {e}")

    """A single stream + visualization unit connected to a shared connector."""

    def __init__(self, name, expr, client):
        self.name = name
        self.expr = expr
        self.client = client
        self.streamdata = self.client.create_stream(expr=expr)
        self.visualizer = None

        # Update control
        self.default_value = 10
        self._last_update = time.time()
        self.update_interval = 0.5  # seconds

        # --- Widgets (persistent references) ---
        self.out = widgets.Output()
        self.slider = widgets.IntSlider(value=self.default_value, min=1, max=100, description="Window Size:")
        self.width_slider = widgets.IntSlider(value=self.default_value, min=1, max=100, description="Window Width:")
        self.colorpicker = widgets.ColorPicker(value="blue", description="Color")
        self.dim_button = widgets.Checkbox(value=True, description="Dim History")
        self.date_button = widgets.Checkbox(value=False, description="Date")
        self.offset_button = widgets.Checkbox(value=False, description="Offset")

        self.button_reset = widgets.Button(description="Reset")
        self.button_apply = widgets.Button(
            description="Please wait",
            tooltip="Apply changes to this visualization",
            disabled=True
        )

        # Build UI
        self.accordion = None
        self.ui = None
        self.build_ui()
        self.register_callbacks()

        # Initialize visualizer once
        with self.out:
            try:
                self.visualizer = tw.Visualizer(
                    self.streamdata,
                    vis_type="line",
                    window_width=int(self.width_slider.value),
                    window_size=int(self.slider.value),
                    Date=bool(self.date_button.value),
                    useOffset=bool(self.offset_button.value),
                    dim_history=bool(self.dim_button.value),
                    color=self.colorpicker.value,
                )
                self.visualizer.show()
            except Exception as e:
                print(f"Visualizer init error for '{self.name}':", e)

    def build_ui(self):
        left_box = widgets.VBox([self.slider, self.width_slider, self.colorpicker])
        right_box = widgets.VBox([self.dim_button, self.date_button, self.offset_button])
        options_box = widgets.HBox([left_box, right_box])
        self.accordion = widgets.Accordion(children=[options_box])
        self.accordion.set_title(0, f"{self.name} Options")

        self.ui = widgets.VBox([
            widgets.HBox([self.button_reset, self.button_apply]),
            self.accordion,
            self.out
        ])

    def register_callbacks(self):
        self.button_reset.on_click(self.reset)
        self.button_apply.on_click(self.apply_with_debounce)
        self.slider.observe(self.apply_with_debounce, names="value")
        self.width_slider.observe(self.apply_with_debounce, names="value")
        self.colorpicker.observe(self.apply_with_debounce, names="value")

    def draw(self):
        display(self.ui)

    def apply_with_debounce(self, _=None):
        now = time.time()
        if now - self._last_update > self.update_interval:
            self.update_visualizer()
            self._last_update = now

    def enable_apply_button(self):
        if self.button_apply.disabled:
            self.button_apply.disabled = False
            self.button_apply.description = "Apply"
            print(f"✅ Apply button enabled for '{self.name}'")

    def reset(self, _=None):
        """Reset UI controls (does not close visualizer)."""
        self.slider.value = self.default_value
        self.width_slider.value = self.default_value
        self.colorpicker.value = "blue"
        self.dim_button.value = True
        self.date_button.value = False
        self.offset_button.value = False

        # Only clear output (visualizer stays)
        with self.out:
            self.out.clear_output(wait=True)
            if self.visualizer:
                try:
                    self.visualizer.show()
                except Exception:
                    pass

    async def _show_async(self):
        """Async-safe visualizer show inside Output widget."""
        with self.out:
            try:
                self.visualizer.show()
                plt.show(block=False)
            except Exception as e:
                print(f"Visualizer async error for '{self.name}':", e)

    def update_visualizer(self):
        """Recreate visualizer from current widget values."""
        if not getattr(self, "streamdata", None):
            return

        with self.out:
            self.out.clear_output(wait=True)

            # Close old figure safely
            if self.visualizer:
                try:
                    plt.close(self.visualizer.fig)
                except Exception:
                    pass

            # Create a new Visualizer with updated parameters
            try:
                self.visualizer = tw.Visualizer(
                    self.streamdata,
                    vis_type="line",
                    window_size=int(self.slider.value),
                    window_width=int(self.width_slider.value),
                    dim_history=bool(self.dim_button.value),
                    Date=bool(self.date_button.value),
                    useOffset=bool(self.offset_button.value),
                    color=self.colorpicker.value,
                )
                self.visualizer.show()
            except Exception as e:
                print(f"Visualizer recreate error: {e}")

# ----------------------------- TWAPI CONTROLLER -----------------------------
class twapi:
    """Main controller for connector + multiple stream/draw units."""

    def __init__(self):
        self.client = tw.WatcherClient()
        self.connector_instance = None
        self.units = {}
        self.apply_enabled = False
        self.metrics_label = widgets.Label(value="")

    def update_metrics(self, metrics: str):
        self.metrics_label.value = metrics

    def enable_apply_button(self):
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

    def draw(self, name):
        if name not in self.units:
            print(f"Unit '{name}' not found.")
            return
        self.units[name].draw()

    def draw_all(self):
        all_uis = [unit.ui for unit in self.units.values()]
        display(widgets.VBox(all_uis))
