import matplotlib
matplotlib.use('ipympl')
import tensorwatchext as tw
from tensorwatchext import kafka_connector as kc
from tensorwatchext import pykafka_connector as pyc
from pykafka.common import OffsetType
from IPython.display import display
from ipywidgets import widgets
import time
import logging
import matplotlib.pyplot as plt
from queue import Queue
import threading

class twapi:
    """TensorWatch API Wrapper for Kafka Streaming and Visualization"""

    def __init__(self):
        """Initializes the twapi class, setting up the UI widgets and event handlers."""
        self.default_value = 10
        self.visualizer = None  # Initialize visualizer as None
        self.streamdata = None
        self.client = None # Will be initialized in the connector method
        self.out = widgets.Output(layout={})
        self.ready_queue = Queue()
        self._stop_event = threading.Event()

        # Initialize UI widgets
        self.update_interval = 0.5  # Delay in seconds
        self.my_slider = widgets.IntSlider(value=self.default_value, min=1, max=100, step=1, description="Window Size:")
        self.my_slider2 = widgets.IntSlider(value=self.default_value, min=1, max=100, step=1, description="Window Width:")
        self.datebutton = widgets.Checkbox(value=False, description="Date")
        self.offsetbutton = widgets.Checkbox(value=False, description="Use Offset")
        self.dimhistorybutton = widgets.Checkbox(value=True, description="Dim History")
        self.colorpicker = widgets.ColorPicker(value="blue", description="Pick a Color")
        self.expr_input = widgets.Text(
            value='',
            placeholder='lambda d: ...',
            description='Expression:',
            layout={'width': '99%'}
        )
        self.initial_expr = "" # To store the starting expression
        
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
        try:
            # Always clear output before drawing
            self.out.clear_output(wait=True)

            # Create the stream using the current expression
            current_expr = self.expr_input.value
            if not current_expr:
                with self.out:
                    print("Expression is empty. Please define a stream expression.")
                return

            # Create the stream using the user-provided expression
            self.streamdata = self.client.create_stream(expr=current_expr)
            if not self.streamdata:
                with self.out:
                    print("Could not create stream. Waiting for data...")
                return

            # Close previous visualizer if it exists to free resources
            if self.visualizer:
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

    def _process_queue(self):
        """
        Safely checks the queue from a dedicated thread to process messages
        from the background connector (e.g., ready status, metrics).
        This avoids direct UI manipulation from the background thread.
        """
        while not self._stop_event.is_set():
            try:
                message = self.ready_queue.get(timeout=1)
                msg_type, payload = message  # Expects a tuple (type, payload)

                if msg_type == "ready":
                    self.enable_apply_button()
                elif msg_type == "metrics":
                    self.update_metrics(payload)
            except Exception:
                # Queue is empty, just continue waiting
                pass

    def reset(self, _=None):
        """Resets all widget values to their defaults and clears the visualization."""
        self.my_slider.value = self.default_value
        self.my_slider2.value = self.default_value
        self.datebutton.value = False
        self.offsetbutton.value = False
        self.dimhistorybutton.value = True
        self.colorpicker.value = "blue"
        self.expr_input.value = self.initial_expr # Reset to the initial expression
        
        # Clear the output and close the visualizer
        self.out.clear_output()
        plt.close('all')
        self.visualizer = None
        self._stop_event.set() # Stop the checker thread

    def draw(self, expr=None):
        """Displays the UI and optionally sets the initial expression."""
        if expr:
            self.expr_input.value = expr
            self.initial_expr = expr # Store the initial expression
        ui = widgets.VBox([
            self.expr_input,
            widgets.HBox([self.button_reset, self.button_apply]),
            self.accordion,
            self.out
        ])
        display(ui)
        # Start the thread that safely checks for the ready signal
        self._stop_event.clear()
        checker_thread = threading.Thread(target=self._process_queue, daemon=True)
        checker_thread.start()

    def draw_with_metrics(self, expr=None):
        """Displays the UI with metrics and optionally sets the initial expression."""
        if expr:
            self.expr_input.value = expr
            self.initial_expr = expr # Store the initial expression
        ui = widgets.VBox([
            self.metrics_label,
            self.expr_input,
            widgets.HBox([self.button_reset, self.button_apply]),
            self.accordion,
            self.out
        ])
        display(ui)
        # Start the thread that safely checks for the ready signal
        self._stop_event.clear()
        checker_thread = threading.Thread(target=self._process_queue, daemon=True)
        checker_thread.start()

    def update_metrics(self, metrics):
        """Updates the metrics label with the provided text."""
        self.metrics_label.value = metrics

    def connector(self, topic, host=None, parsetype="json", cluster_size=1, conn_type="kafka", queue_length=50000,
                  group_id="mygroup", schema_path=None, protobuf_message=None, parser_extra=None,
                  random_sampling=None, countmin_width=None, countmin_depth=None, ordering_field=None,
                  ordering_buffer_size=1000, ordering_timeout=10, poll=1.0, auto_offset="earliest", decode="utf-8",
                  zmq_port=None, consumer_config=None, auto_offset_reset=OffsetType.EARLIEST,
                  fetch_message_max_bytes=1024 * 1024, num_consumer_fetchers=1, auto_commit_enable=False,
                  auto_commit_interval_ms=1000, queued_max_messages=2000, fetch_min_bytes=1,
                  consumer_timeout_ms=-1, consumer_start_timeout_ms=5000, zookeeper_hosts='127.0.0.1:2181'):
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
            ordering_field (str): The field to use for ordering messages.
            ordering_buffer_size (int): The buffer size for ordering messages.
            ordering_timeout (int): The timeout in seconds for ordering messages.
            poll (float): The polling timeout in seconds for the Kafka consumer. (kafka only)
            auto_offset (str): The auto offset reset policy. (kafka only)
            decode (str): The string decoding format. (kafka only)
            zmq_port (int): The ZMQ port for communication. (kafka only)
            consumer_config (dict): Custom configuration for the Kafka consumer. (kafka only)
            auto_offset_reset (OffsetType): Offset reset policy. (pykafka only)
            fetch_message_max_bytes (int): Max message size to fetch. (pykafka only)
            num_consumer_fetchers (int): Number of fetcher threads. (pykafka only)
            auto_commit_enable (bool): Enable auto-commit. (pykafka only)
            auto_commit_interval_ms (int): Auto-commit interval. (pykafka only)
            queued_max_messages (int): Max messages to queue. (pykafka only)
            fetch_min_bytes (int): Min bytes to fetch. (pykafka only)
            consumer_timeout_ms (int): Consumer timeout. (pykafka only)
            consumer_start_timeout_ms (int): Consumer start timeout. (pykafka only)
            zookeeper_hosts (str): Zookeeper hosts. (pykafka only)

        Returns:
            A KafkaConnector or pykafka_connector instance.
        """
        connector_instance = None
        if conn_type == "kafka":
            connector_instance = kc(
                topic=topic, hosts=host, parsetype=parsetype, cluster_size=cluster_size, queue_length=queue_length, group_id=group_id,
                schema_path=schema_path, protobuf_message=protobuf_message, parser_extra=parser_extra,
                random_sampling=random_sampling, countmin_width=countmin_width, countmin_depth=countmin_depth, twapi_instance=self.ready_queue,
                ordering_field=ordering_field, poll=poll, auto_offset=auto_offset, decode=decode, zmq_port=zmq_port, consumer_config=consumer_config)
        elif conn_type == "pykafka":
            connector_instance = pyc(
                topic=topic, hosts=host, parsetype=parsetype, cluster_size=cluster_size, twapi_instance=self.ready_queue, 
                queue_length=queue_length, consumer_group=bytes(group_id, 'utf-8'),
                parser_extra=parser_extra, schema_path=schema_path, protobuf_message=protobuf_message,
                random_sampling=random_sampling, countmin_width=countmin_width, countmin_depth=countmin_depth,
                ordering_field=ordering_field, ordering_buffer_size=ordering_buffer_size, ordering_timeout=ordering_timeout,
                auto_offset_reset=auto_offset_reset, fetch_message_max_bytes=fetch_message_max_bytes,
                num_consumer_fetchers=num_consumer_fetchers, auto_commit_enable=auto_commit_enable,
                auto_commit_interval_ms=auto_commit_interval_ms, queued_max_messages=queued_max_messages,
                fetch_min_bytes=fetch_min_bytes, consumer_timeout_ms=consumer_timeout_ms,
                consumer_start_timeout_ms=consumer_start_timeout_ms, zookeeper_hosts=zookeeper_hosts,
                decode=decode, zmq_port=zmq_port)
        else:
            raise ValueError("Invalid connector type. Choose 'kafka' or 'pykafka'.")

        # Initialize the WatcherClient with the actual port used by the connector's watcher
        if connector_instance:
            actual_port = connector_instance.watcher.port
            self.client = tw.WatcherClient(port=actual_port)

        return connector_instance
