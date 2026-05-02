from kafka import KafkaConsumer
import json
from collections import defaultdict, deque
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_events = defaultdict(deque)

WINDOW_SECONDS = 60
THRESHOLD = 3

for message in consumer:
    tx = message.value

    user_id = tx['user_id']
    tx_time = datetime.fromisoformat(tx['timestamp'])

    events = user_events[user_id]
    events.append(tx_time)

    while events and (tx_time - events[0]).total_seconds() > WINDOW_SECONDS:
        events.popleft()

    if len(events) > THRESHOLD:
        print(f"ALERT: user {user_id} zrobił {len(events)} transakcji w 60s")
        print(tx)
