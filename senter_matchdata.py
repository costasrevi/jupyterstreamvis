import time
import json
import pandas as pd
from kafka import KafkaProducer
import argparse

class WorldCupProducer:
    """
    A Kafka producer that reads the FIFA World Cup dataset and streams
    matches from a specific year to a Kafka topic.
    """
    def __init__(self, topic, hosts="localhost:9093", dataset_path="FIFA World Cup 1930-2022 All Match Dataset.csv"):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=hosts,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.dataset = pd.read_csv(dataset_path, encoding='latin-1')

    def stream_world_cup_matches(self, year=1998, interval=1):
        """
        Streams all matches from the specified World Cup year.

        Args:
            year (int): The year of the World Cup to stream.
            interval (int): The time delay (in seconds) between sending each match.
        """
        # Filter the DataFrame based on the 'tournament Name' column
        world_cup_matches = self.dataset[self.dataset['tournament Name'].str.contains(str(year))]
        
        if world_cup_matches.empty:
            print(f"No data found for the {year} World Cup.")
            return

        print(f"Streaming {len(world_cup_matches)} matches from the {year} World Cup to topic '{self.topic}'...")

        for _, match in world_cup_matches.iterrows():
            message = match.to_dict()
            self.producer.send(self.topic, value=message)
            print(f"Sent Match: {message['Home Team Name']} {message['Home Team Score']} - {message['Away Team Score']} {message['Away Team Name']}")
            time.sleep(interval)
            
        print("Finished streaming all matches.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka producer for streaming World Cup match data.")
    parser.add_argument("--topic", type=str, default="worldcup98", help="The Kafka topic to send messages to.")
    parser.add_argument("--hosts", type=str, default="localhost:9093", help="The Kafka broker hosts.")
    parser.add_argument("--dataset", type=str, default="FIFA World Cup 1930-2022 All Match Dataset.csv", help="The path to the World Cup dataset CSV file.")
    parser.add_argument("--year", type=int, default=1998, help="The year of the World Cup to stream.")
    parser.add_argument("--interval", type=int, default=1, help="The interval in seconds between sending matches.")
    
    args = parser.parse_args()

    try:
        producer = WorldCupProducer(
            topic=args.topic,
            hosts=args.hosts,
            dataset_path=args.dataset
        )
        producer.stream_world_cup_matches(year=args.year, interval=args.interval)
    except FileNotFoundError:
        print(f"Error: The dataset file was not found at '{args.dataset}'.")
    except Exception as e:
        print(f"An error occurred: {e}")