import json
import time
import pandas as pd
from kafka import KafkaProducer
import re

# Kafka configuration
KAFKA_TOPIC = 'fifa-matches'
KAFKA_BROKER = 'localhost:9093'

def create_producer():
    """Creates a Kafka producer."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka producer connected successfully.")
            return producer
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def extract_year(tournament_name):
    """Extracts the year from the tournament name string."""
    match = re.search(r'\d{4}', str(tournament_name))
    return int(match.group(0)) if match else 0

def stream_fifa_data():
    """Reads the FIFA dataset and streams it to Kafka."""
    producer = create_producer()
    
    try:
        df = pd.read_csv('FIFA World Cup 1930-2022 All Match Dataset.csv', encoding='latin-1')
    except FileNotFoundError:
        print("Error: 'FIFA World Cup 1930-2022 All Match Dataset.csv' not found.")
        return

    # Create a 'year' column by extracting it from the tournament name
    df['year'] = df['tournament Name'].apply(extract_year)
    
    # Sort by year to have a chronological stream
    df = df.sort_values(by='year')

    print(f"Starting to stream {len(df)} matches to topic '{KAFKA_TOPIC}'...")

    for _, row in df.iterrows():
        # Skip rows where scores are not valid numbers
        try:
            home_score = int(row['Home Team Score'])
            away_score = int(row['Away Team Score'])
        except (ValueError, TypeError):
            print(f"Skipping row with invalid score: {row['Match Name']}")
            continue

        match_data = {
            'year': int(row['year']),
            'home_team': row['Home Team Name'],
            'away_team': row['Away Team Name'],
            'home_score': home_score,
            'away_score': away_score,
            'total_goals': home_score + away_score,
            'stage': row['Stage Name']
        }
        
        producer.send(KAFKA_TOPIC, value=match_data)
        # print(f"Sent match: {match_data['year']} - {match_data['home_team']} vs {match_data['away_team']}")
        
        # Simulate a real-time stream with a small delay
        time.sleep(0.05)

    producer.flush()
    producer.close()
    print("Finished streaming all matches.")

if __name__ == "__main__":
    stream_fifa_data()