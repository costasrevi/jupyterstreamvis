from pykafka import KafkaClient
from pykafka.common import OffsetType
from tensorwatch import Watcher
import threading
from queue import Queue
import json
import pickle
import time
import random
import logging
import io

# Optional Parsers
try:
    import xmltodict
except ImportError:
    xmltodict = None

try:
    import avro.schema
    from avro.io import DatumReader, BinaryDecoder
except ImportError:
    avro = None

try:
    from protobuf_to_dict import protobuf_to_dict
except ImportError:
    protobuf_to_dict = None

from probables import CountMinSketch

class pykafka_connector(threading.Thread):
    def __init__(self, hosts: str = None, topic: str = None, parsetype: str = None, queue_length: int = None, cluster_size: int = 1,
                 consumer_group: bytes = b'default', auto_offset_reset: OffsetType = OffsetType.EARLIEST,
                 fetch_message_max_bytes: int = 1024 * 1024, num_consumer_fetchers: int = 1,
                 auto_commit_enable: bool = False, auto_commit_interval_ms: int = 1000,
                 queued_max_messages: int = 2000, fetch_min_bytes: int = 1,
                 consumer_timeout_ms: int = -1, decode: str = "utf-8",
                 scema_path: str = None, random_sampling: int = None, countmin_width: int = None,
                 countmin_depth: int = None, twapi_instance=None, parser_extra=None, probuf_message=None, zookeeper_hosts:str='127.0.0.1:2181'):
        super().__init__()
        self.hosts = hosts or "127.0.0.1:9092"
        self.topic = topic
        self.cluster_size = cluster_size
        self.decode = decode
        self.parsetype = parsetype
        self.scema_path = scema_path
        self.random_sampling = random_sampling
        self.parser_extra = parser_extra
        self.probuf_message = probuf_message
        self.queue_length = queue_length
        self.data = Queue(maxsize=queue_length or 50000)
        self._quit = threading.Event()
        self.size = 0
        self.watcher = Watcher()
        self.cms = {}
        self.countmin_depth = countmin_depth
        self.countmin_width = countmin_width

        # pykafka specific settings
        self.consumer_group = consumer_group
        self.auto_offset_reset = auto_offset_reset
        self.fetch_message_max_bytes = fetch_message_max_bytes
        self.num_consumer_fetchers = num_consumer_fetchers
        self.auto_commit_enable = auto_commit_enable
        self.auto_commit_interval_ms = auto_commit_interval_ms
        self.queued_max_messages = queued_max_messages
        self.fetch_min_bytes = fetch_min_bytes
        self.consumer_timeout_ms = consumer_timeout_ms
        self.zookeeper_hosts = zookeeper_hosts

        # twapi integration
        self.twapi_instance = twapi_instance
        self.latencies = []
        self.received_count = 0
        self.last_report_time = time.time()
        self.first_message_sent = False

        # Parsers initialization
        self.reader = None
        self.mymodule = None
        if self.parsetype:
            if self.parsetype.lower() == 'avro' and avro:
                try:
                    schema = avro.schema.parse(parser_extra)
                    self.reader = DatumReader(schema)
                except Exception as e:
                    print(f"Avro schema error or avro not installed: {e}")
            elif self.parsetype.lower() == 'protobuf' and protobuf_to_dict:
                try:
                    import sys
                    import importlib
                    if scema_path:
                        sys.path.append(scema_path)
                    mymodule = importlib.import_module(parser_extra)
                    method_to_call = getattr(mymodule, probuf_message)
                    self.mymodule = method_to_call
                except Exception as e:
                    print(f"Error importing protobuf: {e}")

        self.start()

    def myparser(self, message):
        try:
            if self.parsetype is None or self.parsetype.lower() == 'json':
                return json.loads(message)
            elif self.parsetype.lower() == 'pickle':
                return pickle.loads(message)
            elif self.parsetype.lower() == 'xml' and xmltodict:
                return xmltodict.parse(message).get("root")
            elif self.parsetype.lower() == 'protobuf' and self.mymodule:
                dynamic_message = self.mymodule()
                dynamic_message.ParseFromString(message)
                return protobuf_to_dict(dynamic_message)
            elif self.parsetype.lower() == 'avro' and self.reader:
                message_bytes = io.BytesIO(message)
                decoder = BinaryDecoder(message_bytes)
                return self.reader.read(decoder)
        except Exception as e:
            logging.error(f"Parsing Error ({self.parsetype}): {e}")
        return None

    def process_message(self, message_bytes):
        receive_time = time.time()
        try:
            if self.random_sampling and self.random_sampling > random.randint(0, 100):
                return

            parsed_message = self.myparser(message_bytes)
            if parsed_message is None:
                return

            if isinstance(parsed_message, dict) and 'send_time' in parsed_message:
                self.received_count += 1
                send_time = parsed_message['send_time']
                latency = receive_time - send_time
                self.latencies.append(latency)
                parsed_message['latency'] = latency
                parsed_message['receive_time'] = receive_time

            

            if not self.data.full():
                self.data.put(parsed_message, block=False)
                if not self.first_message_sent and self.twapi_instance:
                    logging.info("First message received, enabling apply button.")
                    self.twapi_instance.enable_apply_button()
                    # self.twapi_instance.apply_with_debounce()
                    self.first_message_sent = True
            # else:
                # logging.warning("Queue is full, dropping message.")

            if isinstance(parsed_message, dict) and self.countmin_width and self.countmin_depth:
                for key, value in parsed_message.items():
                    self.cms.setdefault(key, CountMinSketch(width=self.countmin_width, depth=self.countmin_depth))
                    self.cms[key].add(str(value))

            self.size += 1
        except Exception as e:
            logging.error(f"Message Processing Error: {e}")

    def consumer_loop(self):
        logging.info(f"Starting pykafka consumer loop for topic '{self.topic}'")
        client = KafkaClient(hosts=self.hosts)
        topic = client.topics[self.topic]

        if self.cluster_size > 1:
            consumer = topic.get_balanced_consumer(
                consumer_group=self.consumer_group,
                auto_commit_enable=self.auto_commit_enable,
                auto_offset_reset=self.auto_offset_reset,
                num_consumer_fetchers=self.num_consumer_fetchers,
                auto_commit_interval_ms=self.auto_commit_interval_ms, #
                queued_max_messages=self.queued_max_messages,
                fetch_min_bytes=self.fetch_min_bytes,
                zookeeper_connect=self.zookeeper_hosts
            )
        else:
            consumer = topic.get_simple_consumer(
                auto_offset_reset=self.auto_offset_reset,
                consumer_timeout_ms=self.consumer_timeout_ms,
                fetch_message_max_bytes=self.fetch_message_max_bytes,
                auto_commit_enable=self.auto_commit_enable,
                auto_commit_interval_ms=self.auto_commit_interval_ms,
                queued_max_messages=self.queued_max_messages,
                fetch_min_bytes=self.fetch_min_bytes
            )

        for message in consumer:
            if self._quit.is_set():
                break
            if message is not None:
                self.process_message(message.value)
        
        consumer.stop()
        logging.info("Consumer loop stopped")

    def run(self):
        logging.info(f"Starting {self.cluster_size} pykafka consumer threads")
        threads = [threading.Thread(target=self.consumer_loop, daemon=True) for _ in range(self.cluster_size)]
        for thread in threads:
            thread.start()

        while not self._quit.is_set():
            if not self.data.empty():
                self.watcher.observe(data=list(self.data.queue), size=self.size, cms=self.cms)

            current_time = time.time()
            if current_time - self.last_report_time > 5.0:
                if self.latencies:
                    avg_latency = sum(self.latencies) / len(self.latencies)
                    max_latency = max(self.latencies)
                    min_latency = min(self.latencies)
                    time_since_last_report = current_time - self.last_report_time
                    throughput = self.received_count / time_since_last_report if time_since_last_report > 0 else 0
                    
                    stats_str = (f"Recv Throughput: {throughput:.2f} msgs/s | "
                                 f"Send-Recv Latency (ms): "
                                 f"Avg: {avg_latency*1000:.2f}, "
                                 f"Min: {min_latency*1000:.2f}, "
                                 f"Max: {max_latency*1000:.2f}")
                    
                    if self.twapi_instance:
                        self.twapi_instance.update_metrics(stats_str)

                    self.latencies = []
                    self.received_count = 0
                self.last_report_time = current_time

            time.sleep(0.4)

    def quit(self):
        self._quit.set()