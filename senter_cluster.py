from pykafka import KafkaClient
from pykafka.partitioners import HashingPartitioner
import json
import time
import random
import sys
import argparse
import threading

def producer_thread_worker(thread_id, hosts, topic_name, num_messages, linger_ms):
    """
    A worker function for a single producer thread.
    Connects to Kafka and sends a specified number of messages.
    """
    try:
        # Each thread needs its own KafkaClient instance
        client = KafkaClient(hosts=hosts)
        topic = client.topics[topic_name.encode('utf-8')]
        print(f"[Thread-{thread_id}] Connected to Kafka and got topic '{topic_name}'.")
    except Exception as e:
        print(f"[Thread-{thread_id}] ERROR: Failed to connect to Kafka: {e}")
        return 0

    hashing_partitioner = HashingPartitioner()

    # Use an asynchronous producer for high throughput.
    # linger_ms is crucial for batching messages.
    producer = topic.get_producer(
        partitioner=hashing_partitioner,
        linger_ms=linger_ms,
        min_queued_messages=100 # Start flushing when 100 messages are queued
    )

    start_time = time.time()
    for i in range(num_messages):
        # Create a unique sequence number across all threads
        seq = (thread_id * num_messages) + i
        message = {
            'seq': seq,
            'send_time': time.time(),
            'data': random.randint(0, 1000)
        }

        # The partition key determines which partition the message goes to.
        partition_key = str(message['seq']).encode('utf-8')
        payload = json.dumps(message).encode('utf-8')

        producer.produce(payload, partition_key=partition_key)

        if (i + 1) % 50000 == 0:
            print(f"[Thread-{thread_id}] Produced {i + 1:,} messages...")

    # Crucial: stop() flushes all pending messages before exiting.
    print(f"[Thread-{thread_id}] Flushing final messages...")
    producer.stop()
    end_time = time.time()
    duration = end_time - start_time
    print(f"[Thread-{thread_id}] Finished. Sent {num_messages:,} messages in {duration:.2f}s.")
    return num_messages


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka benchmark producer for a cluster.")
    parser.add_argument("--hosts",
                        type=str,
                        default="127.0.0.1:9093",
                        help="Comma-separated list of Kafka broker hosts (e.g., 'host1:9092,host2:9092').")
    parser.add_argument("--topic", type=str, default="presentation2", help="The Kafka topic to produce messages to.")
    parser.add_argument("--num-threads", type=int, default=1, help="Number of producer threads to run.")
    parser.add_argument("--num-messages", type=int, default=200000, help="The number of messages to send *per thread*.")
    parser.add_argument("--linger-ms", type=int, default=5, help="Time (ms) for the producer to wait and batch messages.")

    args = parser.parse_args()

    threads = []
    total_messages_to_send = args.num_threads * args.num_messages
    print(f"Starting benchmark with {args.num_threads} thread(s)...")
    print(f"Total messages to be sent: {total_messages_to_send:,}")
    
    overall_start_time = time.time()

    for i in range(args.num_threads):
        thread = threading.Thread(target=producer_thread_worker, args=(i, args.hosts, args.topic, args.num_messages, args.linger_ms))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    overall_duration = time.time() - overall_start_time
    throughput = total_messages_to_send / overall_duration if overall_duration > 0 else float('inf')

    print("\n--- OVERALL BENCHMARK SUMMARY ---")
    print(f"Sent a total of {total_messages_to_send:,} messages in {overall_duration:.2f} seconds.")
    print(f"Aggregate throughput: {throughput:,.2f} messages/sec.")
    print("---------------------------------")

