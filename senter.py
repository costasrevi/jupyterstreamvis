from pykafka import KafkaClient
import json
import time
import random
import sys

def main():
    """
    Benchmark producer to test throughput and latency.
    """
    try:
        client = KafkaClient(hosts="127.0.0.1:9093")
        topic = client.topics['gemini4']
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        sys.exit(1)

    parse_type = "json"
    num_messages = 200000  # Number of messages to send

    with topic.get_sync_producer() as producer:
        print("Starting benchmark...")
        print(f"Sending {num_messages} messages to topic '{topic.name.decode()}'...")

        start_time = time.time()

        for i in range(num_messages):
            # The original message format
            message = {
                'seq': i,
                'send_time': time.time(),
                'data': random.randint(0, 1000)
            }

            # # New message format for testing topk
            # batch_size = 20
            # message = {
            #     'labels': [random.randint(0, 9) for _ in range(batch_size)],
            #     'losses': [random.random() for _ in range(batch_size)],
            #     'send_time': time.time() # keep send_time for latency calculation
            # }
            
            if parse_type == "json":
                data = json.dumps(message)
                producer.produce(data.encode('utf-8'))
            
            if (i + 1) % 1000 == 0:
                print(f"Sent {i + 1}/{num_messages} messages...")

        end_time = time.time()
        duration = end_time - start_time
        throughput = num_messages / duration if duration > 0 else float('inf')

        print("\n--- BENCHMARK SUMMARY ---")
        print(f"Sent {num_messages} messages in {duration:.2f} seconds.")
        print(f"Producer throughput: {throughput:.2f} messages/sec.")
        print("------------------------")

if __name__ == "__main__":
    main()