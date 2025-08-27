import time
import threading
from queue import Queue
import random
import json
import pickle
from typing import Dict
from confluent_kafka import Consumer
from tensorwatch import Watcher
from probables import CountMinSketch

# Optional Parsers
try:
    import xmltodict
except ImportError:
    xmltodict = None

try:
    import avro.schema
    from avro.io import DatumReader, BinaryDecoder
    import io
except ImportError:
    avro = None

try:
    from protobuf_to_dict import protobuf_to_dict
    from google.protobuf import message
except ImportError:
    protobuf_to_dict = None

# Kafka Consumer Class
class KafkaConnector(threading.Thread):
    def __init__(self, hosts="localhost:9092", topic=None, parsetype=None, avro_schema=None, queue_length=50000,
                 cluster_size=1, consumer_config=None, poll=1.0, auto_offset="earliest", group_id="mygroup",
                 decode="utf-8", schema_path=None, protobuf_message=None, random_sampling=None, countmin_width=None,
                 countmin_depth=None):
        super().__init__()
        self.hosts = hosts or "localhost:9092"
        self.topic = topic
        self.cluster_size = cluster_size
        self.decode = decode
        self.parsetype = parsetype
        self.protobuf_message = protobuf_message
        self.queue_length = queue_length
        self.data = Queue(maxsize=queue_length)
        self.cms = {}  # Count-Min Sketch table
        self.countmin_width = countmin_width
        self.countmin_depth = countmin_depth
        self.random_sampling = random_sampling
        self.poll = poll
        self.consumer_config = consumer_config or {
            "bootstrap.servers": self.hosts,
            "group.id": group_id,
            "auto.offset.reset": auto_offset,
        }
        self._quit = threading.Event()
        self.size = 0
        self.watcher = Watcher()
        self.schema = None
        self.reader = None

        # Load Avro Schema if needed
        if parsetype == "avro" and avro:
            try:
                self.schema = avro.schema.parse(avro_schema)
                self.reader = DatumReader(self.schema)
            except Exception as e:
                print(f"Avro Schema Error: {e}, Avro may not work")
                return

        # Load Protobuf if needed
        if parsetype == "protobuf" and protobuf_to_dict:
            try:
                import importlib
                module = importlib.import_module(protobuf_message)
                self.protobuf_class = getattr(module, protobuf_message)

            except Exception as e:
                print(f"Protobuf Import Error: {e}")
                self.protobuf_class = None

        self.start()

    def myparser(self, message):
        """Parse messages based on the specified format."""
        try:
            if self.parsetype is None or self.parsetype.lower() == "json":
                return json.loads(message)
            elif self.parsetype.lower() == "pickle":
                return pickle.loads(message)
            elif self.parsetype.lower() == "xml" and xmltodict:
                return xmltodict.parse(message)["root"]
            elif self.parsetype.lower() == "protobuf" and protobuf_to_dict:
                if self.protobuf_class:
                    dynamic_message = self.protobuf_class()
                    dynamic_message.ParseFromString(message)
                    return protobuf_to_dict(dynamic_message)

            elif self.parsetype.lower() == "avro" and avro:
                decoder = BinaryDecoder(io.BytesIO(message))
                return self.reader.read(decoder)
        except Exception as e:
            print(f"Parsing Error ({self.parsetype}): {e}")
        return None

    def process_message(self, msg):
        """Processes a single message from Kafka."""
        try:
            if self.random_sampling and self.random_sampling > random.randint(0, 100):
                return
            
            message = msg.value().decode(self.decode)
            parsed_message = self.myparser(message)

            if parsed_message and not self.data.full():
                self.data.put(parsed_message, block=False)

            if isinstance(parsed_message, dict) and self.countmin_width and self.countmin_depth:
                for key, value in parsed_message.items():
                    self.cms.setdefault(key, CountMinSketch(width=self.countmin_width, depth=self.countmin_depth))
                    self.cms[key].add(str(value))

            self.size += 1
        except Exception as e:
            print(f"Message Processing Error: {e}, Message: {message}")

    def consumer_loop(self):
        """Kafka Consumer loop that fetches messages and processes them."""
        consumer = Consumer(self.consumer_config)
        consumer.subscribe([self.topic])

        while not self._quit.is_set():
            msg = consumer.poll(self.poll)
            if msg and not msg.error():
                self.process_message(msg)

        consumer.close()

    def run(self):
        """Start multiple consumer threads if needed."""
        if self.cluster_size == 1:
            self.consumer_loop()
        else:
            threads = [threading.Thread(target=self.consumer_loop, daemon=True) for _ in range(self.cluster_size)]
            for thread in threads:
                thread.start()
            while not self._quit.is_set():
                self.watcher.observe(data=list(self.data.queue), size=self.size, cms=self.cms)
                time.sleep(0.4)
