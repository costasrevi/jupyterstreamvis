import tensorwatchext as tw
from IPython.display import display
from ipywidgets import widgets
import matplotlib.pyplot as plt
import time
import asyncio


class StreamUnit:
    """A single stream + visualization unit connected to a shared connector."""

    def __init__(self, name, expr, client):
        self.name = name
        self.expr = expr
        self.client = client
        self.streamdata = self.client.create_stream(expr=expr)
        self.visualizer = None
        self.first_plot_shown = False  # ✅ Track first plot for deletion

        # Update control
        self.default_value = 10
        self._last_update = time.time()
        self.update_interval = 0.5  # seconds

        # --- Widgets ---
        self.out = widgets.Output()
        self.slider = widgets.IntSlider(value=self.default_value, min=1, max=100, description="Window Size:")
        self.width_slider = widgets.IntSlider(value=self.default_value, min=1, max=100, description="Window Width:")
        self.colorpicker = widgets.ColorPicker(value="blue", description="Color")
        self.dim_button = widgets.Checkbox(value=True, description="Dim History")
        self.date_button = widgets.Checkbox(value=False, description="Date")
        self.offset_button = widgets.Checkbox(value=False, description="Offset")

        self.button_reset = widgets.Button(description="Reset")
        self.button_apply = widgets.Button(description="Please wait", tooltip="Apply changes", disabled=True)

        # Build UI
        self.build_ui()
        self.register_callbacks()

        # Initialize visualizer once
        self._init_visualizer()

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

    def enable_apply_button(self):
        if self.button_apply.disabled:
            self.button_apply.disabled = False
            self.button_apply.description = "Apply"
            print(f"✅ Apply button enabled for '{self.name}'")

    def reset(self, _=None):
        """Reset UI controls and refresh visualizer."""
        self.slider.value = self.default_value
        self.width_slider.value = self.default_value
        self.colorpicker.value = "blue"
        self.dim_button.value = True
        self.date_button.value = False
        self.offset_button.value = False
        self.apply_with_debounce()

    def apply_with_debounce(self, _=None):
        now = time.time()
        if now - self._last_update > self.update_interval:
            self.update_visualizer()
            self._last_update = now

    async def _show_async(self):
        with self.out:
            try:
                self.out.clear_output(wait=True)
                self.visualizer.show()
                plt.show(block=False)
            except Exception as e:
                print(f"Visualizer async error for '{self.name}': {e}")

    def _init_visualizer(self):
        """Initial visualizer for first plot."""
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
                self.first_plot_shown = True
            except Exception as e:
                print(f"Visualizer init error for '{self.name}': {e}")

    def update_visualizer(self):
        """Update visualizer parameters in place (no new figure)."""
        if not self.visualizer:
            return
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
            print(f"Visualizer recreate error for '{self.name}': {e}")
