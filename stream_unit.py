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
        self.visualizer_show = False

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
        """Create or update visualizer inside the Output widget."""
        if not getattr(self, "streamdata", None):
            return

        # 1. Close the old figure if it exists. This targets the specific plot.
        if self.visualizer and hasattr(self.visualizer, 'fig'):
            plt.close(self.visualizer.fig)
        
        # 2. Clear the output widget area for this unit.
        self.out.clear_output(wait=True)
        
        # 3. Draw the new plot into the clean output area.
        with self.out:


            # Create new Visualizer with current widget values
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

                # Async display
                self.visualizer.show()

            except Exception as e:
                print(f"Visualizer error for '{self.name}': {e}")