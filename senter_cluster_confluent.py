from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
import time
import random
import argparse
import threading

# Import serialization libraries with error handling
try:
    # Conditional import: relative for module execution, absolute for script execution.
    if __package__:
        from .benchmark_pb2 import BenchmarkMessage
    else:
        from benchmark_pb2 import BenchmarkMessage
except ImportError:
    BenchmarkMessage = None

try:
    import avro.schema
    import avro.io
except ImportError:
    avro = None

import io
import pickle
import json


def ensure_topic_partitions(hosts, topic_name, num_threads):
    admin = AdminClient({"bootstrap.servers": hosts})
    metadata = admin.list_topics(timeout=5)

    if topic_name in metadata.topics:
        current_partitions = len(metadata.topics[topic_name].partitions)
        print(f"🧩 Topic '{topic_name}' exists with {current_partitions} partition(s).")
        if current_partitions < num_threads:
            print(f"🔧 Increasing partitions to {num_threads} to match threads...")
            fs = admin.create_partitions([NewPartitions(topic_name, num_threads)])
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"✅ Updated '{topic}' partitions → {num_threads}")
                except Exception as e:
                    print(f"⚠️ Could not update partitions: {e}")
        else:
            print(f"✅ Topic already has sufficient partitions ({current_partitions}).")
    else:
        print(f"📦 Creating topic '{topic_name}' with {num_threads} partitions...")
        new_topic = NewTopic(topic_name, num_partitions=num_threads, replication_factor=1)
        fs = admin.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                print(f"✅ Created topic '{topic}' with {num_threads} partitions.")
            except Exception as e:
                print(f"❌ Failed to create topic: {e}")


def create_serializer(format_type):
    """Returns a serialization function based on the specified format."""
    if format_type == 'protobuf':
        if not BenchmarkMessage:
            raise ImportError("Could not import BenchmarkMessage. Please generate it from benchmark.proto.")
        def serialize(msg_dict):
            message = BenchmarkMessage()
            message.seq = msg_dict['seq']
            message.send_time = msg_dict['send_time']
            message.data = msg_dict['data']
            return message.SerializeToString()
        return serialize

    elif format_type == 'avro':
        if not avro:
            raise ImportError("The 'avro' library is required for Avro serialization. Please run 'pip install avro'.")
        avro_schema_str = """
        {"namespace": "example.avro", "type": "record", "name": "Benchmark",
         "fields": [ {"name": "seq", "type": "long"}, {"name": "send_time", "type": "double"}, {"name": "data", "type": "int"} ]}
        """
        parsed_schema = avro.schema.make_avsc_object(avro_schema_str)
        writer = avro.io.DatumWriter(parsed_schema)
        def serialize(msg_dict):
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(msg_dict, encoder)
            return bytes_writer.getvalue()
        return serialize

    elif format_type == 'pickle':
        return lambda msg_dict: pickle.dumps(msg_dict, protocol=4)

    else: # Default to JSON
        return lambda msg_dict: json.dumps(msg_dict).encode('utf-8')

def producer_thread_worker(thread_id, hosts, topic_name, num_messages, linger_ms, serializer, target_tps):
    conf = {
        "bootstrap.servers": hosts,
        "linger.ms": linger_ms,
        "batch.num.messages": 10,
        "queue.buffering.max.messages": 10000000,
        "queue.buffering.max.kbytes": 512000,
        "acks": 1,
        "compression.type": "none",
        "enable.idempotence": False,
    }

    producer = Producer(conf)
    print(f"[Thread-{thread_id}] Connected to Kafka ({hosts}), producing to topic '{topic_name}'.")

    start_time = time.perf_counter()
    sent = 0

    for i in range(num_messages):
        seq = (thread_id * num_messages) + i
        message_dict = {'seq': seq, 'send_time': time.time(), 'data': random.randint(0, 1000)}
        payload = serializer(message_dict)

        producer.produce(topic=topic_name, key=str(seq), value=payload)
        producer.poll(0)

        sent += 1

        # ⏱ Throughput control
        if target_tps > 0:
            expected_duration = sent / target_tps
            actual_duration = time.perf_counter() - start_time
            if actual_duration < expected_duration:
                time.sleep(expected_duration - actual_duration)

        if (i + 1) % 50000 == 0:
            print(f"[Thread-{thread_id}] Produced {i + 1:,} messages...")

    producer.flush()
    duration = time.perf_counter() - start_time
    print(f"[Thread-{thread_id}] Finished sending {num_messages:,} messages in {duration:.2f}s "
          f"({num_messages/duration:,.2f} msg/s).")
    return num_messages


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified Kafka Producer for various serialization formats.")
    parser.add_argument("--hosts", type=str, default="127.0.0.1:9092", help="Kafka broker list")
    parser.add_argument("--topic", type=str, default="presentation", help="Kafka topic name")
    parser.add_argument("--format", type=str, default="protobuf", choices=['protobuf', 'avro', 'pickle', 'json'], help="Serialization format")
    parser.add_argument("--num-threads", type=int, default=1, help="Number of producer threads")
    parser.add_argument("--num-messages", type=int, default=5000000, help="Messages per thread")
    parser.add_argument("--linger-ms", type=int, default=1, help="Batch linger time (ms)")
    parser.add_argument("--target-tps", type=float, default=0, help="Target throughput per thread (messages/sec, 0=unlimited)")
    args = parser.parse_args()

    try:
        print(f"\n⚙️  Serializer: {args.format.upper()}")
        serializer_func = create_serializer(args.format)

        print(f"⚙️  Ensuring topic '{args.topic}' has at least {args.num_threads} partitions...")
        ensure_topic_partitions(args.hosts, args.topic, args.num_threads)

        threads = []
        total_messages = args.num_threads * args.num_messages
        print(f"\n🚀 Starting {args.num_threads} thread(s) to send {total_messages:,} total messages...")
        overall_start = time.perf_counter()

        for i in range(args.num_threads):
            t = threading.Thread(
                target=producer_thread_worker,
                args=(i, args.hosts, args.topic, args.num_messages, args.linger_ms, serializer_func, args.target_tps)
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        total_duration = time.perf_counter() - overall_start
        throughput = total_messages / total_duration
        print(f"\n📊 Total: {total_messages:,} messages sent in {total_duration:.2f}s")
        print(f"⚡ Aggregate Throughput: {throughput:,.2f} messages/sec")

    except (ImportError, Exception) as e:
        print(f"\n❌ An error occurred: {e}")
