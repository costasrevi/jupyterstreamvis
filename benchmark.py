import time
import json
import threading
import numpy as np
from . import twapi
from . import kafka_connector as kc

class Benchmark:
    def __init__(self, topic, hosts="localhost:9092"):
        self.topic = topic
        self.hosts = hosts
        self.consumer = twapi()
        # The expression creates a KafkaConnector which will consume from the topic.
        # The stream will contain items from the Kafka topic.
        self.stream = self.consumer.stream(expr=f'lambda: kc.KafkaConnector(topic="{self.topic}", hosts="{self.hosts}").data.queue')

        self.latencies = []
        self.throughputs = []
        self.msg_count = 0
        self.start_time = None

    def start(self):
        self.start_time = time.time()
        self.consumer_thread = threading.Thread(target=self._consume)
        self.metrics_thread = threading.Thread(target=self._calculate_metrics)

        self.consumer_thread.start()
        self.metrics_thread.start()

        self.consumer.draw_with_metrics()

    def _consume(self):
        # This loop will block until a message is received
        for msg in self.stream.streamdata.read_all():
            # Assuming the message from Kafka is a JSON string with a "timestamp" field
            # as produced by the original producer.
            latency = time.time() - msg.value["timestamp"]
            self.latencies.append(latency)
            self.msg_count += 1

    def _calculate_metrics(self):
        while True:
            if self.start_time and self.msg_count > 0:
                elapsed_time = time.time() - self.start_time
                throughput = self.msg_count / elapsed_time
                self.throughputs.append(throughput)

                avg_latency = np.mean(self.latencies)
                avg_throughput = np.mean(self.throughputs)

                metrics_str = f"Avg Latency: {avg_latency:.4f}s | Avg Throughput: {avg_throughput:.2f} msg/s"
                self.consumer.update_metrics(metrics_str)

            time.sleep(1)

if __name__ == "__main__":
    # IMPORTANT: Replace "your_kafka_host:9092" with the actual address of your Kafka broker.
    # For example: "192.168.1.100:9092"
    kafka_host = "192.168.68.110:9093"
    
    # The topic should match the topic your producer is writing to.
    topic_name = "benchmarknewlatest"

    print(f"Starting benchmark consumer for topic '{topic_name}' on host '{kafka_host}'")
    benchmark = Benchmark(topic=topic_name, hosts=kafka_host)
    benchmark.start()
